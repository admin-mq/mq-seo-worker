import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY env vars.");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

const WORKER_ID = `worker-${Math.random().toString(16).slice(2)}`;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function normalizeUrl(url) {
  try {
    const u = new URL(url.trim());
    u.hash = "";
    // remove common tracking params
    ["gclid", "fbclid"].forEach((p) => u.searchParams.delete(p));
    [...u.searchParams.keys()].forEach((k) => {
      if (k.toLowerCase().startsWith("utm_")) u.searchParams.delete(k);
    });
    // normalize host to lowercase
    u.hostname = u.hostname.toLowerCase();
    // normalize trailing slash (keep no trailing slash except root)
    if (u.pathname !== "/" && u.pathname.endsWith("/")) {
      u.pathname = u.pathname.slice(0, -1);
    }
    return u.toString();
  } catch {
    return url.trim();
  }
}

function extractBetween(html, start, end) {
  const i = html.indexOf(start);
  if (i === -1) return null;
  const j = html.indexOf(end, i + start.length);
  if (j === -1) return null;
  return html.slice(i + start.length, j).trim();
}

function stripTags(s) {
  return s.replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim();
}

function extractSeo(html, finalUrl) {
  // title
  const titleMatch = html.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
  const titleText = titleMatch ? titleMatch[1].replace(/<[^>]*>/g, " ").trim() : "";
  const hasTitle = titleText.length > 0;

  // meta description
  const metaMatch = html.match(
    /<meta\s+[^>]*name=["']description["'][^>]*content=["']([^"']*)["'][^>]*>/i
  );
  const hasMeta = !!(metaMatch && metaMatch[1].trim().length > 0);

  // h1
  const h1Matches = html.match(/<h1\b[^>]*>/gi);
  const hasH1 = (h1Matches ? h1Matches.length : 0) > 0;

  // meta robots noindex check (very basic)
  const robotsMatch = html.match(
    /<meta\s+[^>]*name=["']robots["'][^>]*content=["']([^"']*)["'][^>]*>/i
  );
  const robotsContent = robotsMatch ? robotsMatch[1].toLowerCase() : "";
  const indexable = !robotsContent.includes("noindex");

  // canonical check (basic)
  const canonMatch = html.match(/<link\s+[^>]*rel=["']canonical["'][^>]*href=["']([^"']+)["'][^>]*>/i);
  let canonicalOk = true;
  if (canonMatch && canonMatch[1]) {
    try {
      const canon = new URL(canonMatch[1], finalUrl).toString();
      canonicalOk = canon === finalUrl;
    } catch {
      canonicalOk = false;
    }
  }

  // schema types (json-ld)
  const schemaTypes = [];
  const jsonLdMatches = [...html.matchAll(/<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi)];
  for (const m of jsonLdMatches) {
    try {
      const raw = m[1].trim();
      if (!raw) continue;
      const parsed = JSON.parse(raw);
      const items = Array.isArray(parsed) ? parsed : [parsed];
      for (const it of items) {
        const t = it && it["@type"];
        if (typeof t === "string") schemaTypes.push(t);
        else if (Array.isArray(t)) schemaTypes.push(...t.filter(x => typeof x === "string"));
      }
    } catch {
      // ignore invalid JSON-LD blocks
    }
  }

  // simple structural score (0–100)
  let structuralScore = 100;
  if (!hasTitle) structuralScore -= 25;
  if (!hasMeta) structuralScore -= 15;
  if (!hasH1) structuralScore -= 15;
  if (!indexable) structuralScore -= 30;
  if (!canonicalOk) structuralScore -= 15;
  structuralScore = Math.max(0, Math.min(100, structuralScore));

  return {
    hasTitle,
    hasMeta,
    hasH1,
    indexable,
    canonicalOk,
    schemaTypes: Array.from(new Set(schemaTypes)),
    structuralScore,
  };
}

async function getNextQueuedJob() {
  const { data, error } = await supabase
    .from("scc_crawl_jobs")
    .select("*")
    .eq("status", "queued")
    .order("created_at", { ascending: true })
    .limit(1)
    .maybeSingle();

  if (error) throw error;
  return data;
}

async function setSnapshotStep(snapshot_id, step) {
  await supabase
    .from("scc_snapshots")
    .update({ progress_step: step })
    .eq("id", snapshot_id);
}

async function upsertPage(site_id, url) {
  // Your table expects unique (site_id, url)
  const { data, error } = await supabase
    .from("scc_pages")
    .upsert(
      { site_id, url },
      { onConflict: "site_id,url" }
    )
    .select("id, url")
    .maybeSingle();

  if (error) throw error;
  return data;
}

async function upsertPageMetrics(snapshot_id, page_id, seo, depth) {
  const payload = {
    snapshot_id,
    page_id,

    indexable: seo.indexable,
    canonical_ok: seo.canonicalOk,
    has_title: seo.hasTitle,
    has_meta: seo.hasMeta,
    has_h1: seo.hasH1,

    schema_types: seo.schemaTypes,          // jsonb
    internal_link_depth: depth,             // integer

    structural_score: seo.structuralScore,  // integer

    // leave these as null for now (until you integrate GSC/GA/Ads)
    // impressions, clicks, avg_position, ctr, etc.
  };

  const { error } = await supabase
    .from("scc_page_snapshot_metrics")
    .upsert(payload, { onConflict: "snapshot_id,page_id" });

  if (error) throw error;
}

async function insertBasicActions(snapshot_id, page_id, seo) {
  const actions = [];

  if (!seo.hasTitle) {
    actions.push({
      snapshot_id,
      page_id,
      action_type: "missing_title",
      title: "Add a unique title tag",
      why_it_matters: "Title tags influence rankings and click-through rate.",
      technical_reason: "No <title> tag was found on the page.",
      severity: "high",
      priority: "high",
      status: "open",
      steps: ["Add a descriptive, keyword-relevant title (50–60 chars)."],
    });
  }

  if (!seo.hasMeta) {
    actions.push({
      snapshot_id,
      page_id,
      action_type: "missing_meta_description",
      title: "Add a meta description",
      why_it_matters: "Meta descriptions improve CTR and clarify page relevance.",
      technical_reason: "No meta description tag was found on the page.",
      severity: "medium",
      priority: "medium",
      status: "open",
      steps: ["Write a clear description (120–160 chars) with primary intent."],
    });
  }

  if (actions.length === 0) return;

  const { error } = await supabase.from("scc_actions").insert(actions);
  if (error) throw error;
}

async function fetchHtml(url) {
  const res = await fetch(url, {
    redirect: "follow",
    headers: {
      "user-agent":
        "Mozilla/5.0 (compatible; MarketersQuestSEO/1.0; +https://marketersquest.com)",
      accept: "text/html,application/xhtml+xml",
    },
  });

  const contentType = res.headers.get("content-type") || "";
  const finalUrl = res.url;

  if (!contentType.toLowerCase().includes("text/html")) {
    return { ok: true, status: res.status, contentType, finalUrl, html: null };
  }

  const html = await res.text();
  return { ok: res.ok, status: res.status, contentType, finalUrl, html };
}

async function run() {
  console.log("MQ SEO Worker started:", WORKER_ID);

  while (true) {
    try {
      const job = await getNextQueuedJob();

      if (!job) {
        await sleep(2000);
        continue;
      }

      console.log("Picked job:", job.id, "snapshot:", job.snapshot_id);

      await supabase.rpc("scc_start_crawl_job", { p_job_id: job.id });

      // Stage steps
      await setSnapshotStep(job.snapshot_id, "discovering");

      // Enqueue ONLY seed url for Stage-1
      const seed = normalizeUrl(job.seed_url);
      await supabase.rpc("scc_enqueue_urls", {
        p_job_id: job.id,
        p_site_id: job.site_id,
        p_snapshot_id: job.snapshot_id,
        p_urls: [seed],
        p_url_normalized: [seed],
        p_depth: 0,
      });

      await setSnapshotStep(job.snapshot_id, "analyzing");

      // Claim 1 URL
      const { data: q, error: claimErr } = await supabase.rpc(
        "scc_claim_next_url",
        { p_job_id: job.id, p_worker_id: WORKER_ID, p_lock_minutes: 10 }
      );
      if (claimErr) throw claimErr;

      if (!q) {
        // Nothing to crawl
        await supabase.rpc("scc_complete_crawl_job", {
          p_job_id: job.id,
          p_success: true,
        });
        continue;
      }

      // Fetch
      const fetched = await fetchHtml(q.url);

      // Mark queue row done/error (basic)
      await supabase.rpc("scc_mark_url_result", {
        p_queue_id: q.id,
        p_success: true,
        p_http_status: fetched.status,
        p_content_type: fetched.contentType,
        p_final_url: fetched.finalUrl,
        p_canonical_url: null,
        p_error: null,
      });

      if (fetched.html) {
        const seo = extractSeo(fetched.html, fetched.finalUrl || q.url);

        // Upsert page entity
        const page = await upsertPage(job.site_id, normalizeUrl(fetched.finalUrl || q.url));

        // Upsert metrics (you may need to rename columns)
        await upsertPageMetrics(job.snapshot_id, page.id, seo, q.depth);

        // Insert 1-2 actions
        await insertBasicActions(job.snapshot_id, page.id, seo);
      }

      await setSnapshotStep(job.snapshot_id, "finalizing");

      await supabase.rpc("scc_complete_crawl_job", {
        p_job_id: job.id,
        p_success: true,
      });

      console.log("Completed job:", job.id);
    } catch (e) {
      console.error("Worker loop error:", e?.message || e);
      await sleep(3000);
    }
  }
}

run();
