import { createClient } from "@supabase/supabase-js";
import axios from "axios";

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

/**
 * Heartbeat config
 */
const HEARTBEAT_EVERY_MS = 15000; // 15s
let lastHeartbeatAt = 0;

async function heartbeat(jobId) {
  const now = Date.now();
  if (now - lastHeartbeatAt < HEARTBEAT_EVERY_MS) return;
  lastHeartbeatAt = now;

  // If your RPC signature differs, change this call accordingly.
  const { error } = await supabase.rpc("scc_job_heartbeat", {
    p_job_id: jobId,
    p_worker_id: WORKER_ID,
  });

  if (error) {
    // heartbeat failures should never crash the worker
    console.error("Heartbeat error:", error.message || error);
  }
}

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

function stripTags(s) {
  return s.replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim();
}

function extractSeo(html, finalUrl) {
  // title
  const titleMatch = html.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
  const titleText = titleMatch ? stripTags(titleMatch[1]) : "";
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
  const canonMatch = html.match(
    /<link\s+[^>]*rel=["']canonical["'][^>]*href=["']([^"']+)["'][^>]*>/i
  );
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
  const jsonLdMatches = [
    ...html.matchAll(
      /<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi
    ),
  ];
  for (const m of jsonLdMatches) {
    try {
      const raw = m[1].trim();
      if (!raw) continue;
      const parsed = JSON.parse(raw);
      const items = Array.isArray(parsed) ? parsed : [parsed];
      for (const it of items) {
        const t = it && it["@type"];
        if (typeof t === "string") schemaTypes.push(t);
        else if (Array.isArray(t))
          schemaTypes.push(...t.filter((x) => typeof x === "string"));
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

async function setSnapshotStep(snapshot_id, step) {
  const { error } = await supabase
    .from("scc_snapshots")
    .update({ progress_step: step })
    .eq("id", snapshot_id);

  if (error) console.error("setSnapshotStep error:", error.message || error);
}

async function upsertPage(site_id, url) {
  const { data, error } = await supabase
    .from("scc_pages")
    .upsert({ site_id, url }, { onConflict: "site_id,url" })
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
    schema_types: seo.schemaTypes, // jsonb
    internal_link_depth: depth,
    structural_score: seo.structuralScore,
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
      summary: "Missing title tag",
      title: "Add a unique title tag",
      why_it_matters: "Title tags influence rankings and click-through rate.",
      technical_reason: "No <title> tag was found on the page.",
      expected_impact_range: "Medium",
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
      summary: "Missing meta description",
      title: "Add a meta description",
      why_it_matters: "Meta descriptions can improve CTR and clarify relevance.",
      technical_reason: "No meta description tag was found on the page.",
      expected_impact_range: "Low–Medium",
      severity: "medium",
      priority: "medium",
      status: "open",
      steps: ["Write a clear description (120–160 chars) matching the page intent."],
    });
  }

  if (!seo.hasH1) {
    actions.push({
      snapshot_id,
      page_id,
      action_type: "missing_h1",
      summary: "Missing H1",
      title: "Add an H1 heading",
      why_it_matters: "H1 helps clarify page topic for users and search engines.",
      technical_reason: "No <h1> tag was found on the page.",
      expected_impact_range: "Low–Medium",
      severity: "low",
      priority: "low",
      status: "open",
      steps: ["Add one clear H1 describing the page topic."],
    });
  }

  if (actions.length === 0) return;

  const { error } = await supabase.from("scc_actions").insert(actions);
  if (error) throw error;
}

async function fetchHtml(url) {
  try {
    const res = await axios.get(url, {
      timeout: 15000,
      maxRedirects: 5,
      headers: {
        "user-agent":
          "Mozilla/5.0 (compatible; MarketersQuestSEO/1.0; +https://marketersquest.com)",
        accept: "text/html,application/xhtml+xml",
      },
      validateStatus: () => true, // don't throw on 404/500
    });

    const contentType = (res.headers?.["content-type"] || "").toLowerCase();
    const finalUrl = (res.request?.res && res.request.res.responseUrl) || url;

    if (!contentType.includes("text/html")) {
      return { ok: true, status: res.status, contentType, finalUrl, html: null };
    }

    return {
      ok: res.status >= 200 && res.status < 400,
      status: res.status,
      contentType,
      finalUrl,
      html: typeof res.data === "string" ? res.data : null,
    };
  } catch (err) {
    const msg =
      err?.code === "ECONNABORTED"
        ? "timeout after 15s"
        : (err?.message || String(err));
    return { ok: false, status: null, contentType: null, finalUrl: url, html: null, error: msg };
  }
}

/**
 * B4: Rescue stale jobs + atomically claim next queued job
 */
async function rescueStaleJobs() {
  const { data, error } = await supabase.rpc("scc_rescue_stale_jobs", { p_minutes: 10 });
  if (error) {
    console.error("Rescue stale jobs error:", error.message || error);
    return;
  }
  if (data && data > 0) console.log("Rescued stale jobs:", data);
}

async function claimNextJob() {
  const { data, error } = await supabase.rpc("scc_claim_next_job", {
    p_worker_id: WORKER_ID,
  });

  if (error) throw error;
  return data || null;
}

async function failJob(jobId, message) {
  try {
    await supabase.rpc("scc_complete_crawl_job", {
      p_job_id: jobId,
      p_success: false,
      p_error: message || "worker error",
    });
  } catch (e) {
    console.error("failJob rpc failed:", e?.message || e);
  }
}

async function run() {
  console.log("MQ SEO Worker started:", WORKER_ID);

  let loopCount = 0;

  while (true) {
    try {
      loopCount += 1;

      // Rescue every ~30 loops to keep DB clean
      if (loopCount % 30 === 1) {
        await rescueStaleJobs();
      }

      // Atomically claim a queued job
      const job = await claimNextJob();

      if (!job) {
        // Optional debug line
        // console.log("No queued jobs. Sleeping...");
        await sleep(2000);
        continue;
      }

      lastHeartbeatAt = 0; // reset heartbeat timer per job
      console.log("Picked job:", job.id, "snapshot:", job.snapshot_id);

      // Heartbeat immediately after picking
      await heartbeat(job.id);

      // Start job (keeps your existing DB workflow)
      await supabase.rpc("scc_start_crawl_job", { p_job_id: job.id });
      await heartbeat(job.id);

      // Stage steps
      await setSnapshotStep(job.snapshot_id, "discovering");
      await heartbeat(job.id);

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
      await heartbeat(job.id);

      await setSnapshotStep(job.snapshot_id, "analyzing");
      await heartbeat(job.id);

      // Claim 1 URL
      const { data: q, error: claimErr } = await supabase.rpc("scc_claim_next_url", {
        p_job_id: job.id,
        p_worker_id: WORKER_ID,
        p_lock_minutes: 10,
      });
      if (claimErr) throw claimErr;

      if (!q) {
        await supabase.rpc("scc_complete_crawl_job", { p_job_id: job.id, p_success: true });
        console.log("Completed job (no urls):", job.id);
        continue;
      }

      await heartbeat(job.id);

      // Fetch
      const fetched = await fetchHtml(q.url);

      if (!fetched.ok) {
        console.error(
          "Fetch failed for:",
          q.url,
          "reason:",
          fetched.error,
          "status:",
          fetched.status
        );

        await supabase.rpc("scc_mark_url_result", {
          p_queue_id: q.id,
          p_success: false,
          p_http_status: null,
          p_content_type: null,
          p_final_url: q.url,
          p_canonical_url: null,
          p_error: fetched.error || "fetch failed",
        });

        await failJob(job.id, `Fetch failed: ${fetched.error || "unknown"}`);
        continue;
      }

      await heartbeat(job.id);

      // Mark queue row done
      await supabase.rpc("scc_mark_url_result", {
        p_queue_id: q.id,
        p_success: true,
        p_http_status: fetched.status,
        p_content_type: fetched.contentType,
        p_final_url: fetched.finalUrl,
        p_canonical_url: null,
        p_error: null,
      });

      await heartbeat(job.id);

      if (fetched.html) {
        const seo = extractSeo(fetched.html, fetched.finalUrl || q.url);

        const page = await upsertPage(job.site_id, normalizeUrl(fetched.finalUrl || q.url));
        await heartbeat(job.id);

        await upsertPageMetrics(job.snapshot_id, page.id, seo, q.depth);
        await heartbeat(job.id);

        // Actions should never block completion
        try {
          await insertBasicActions(job.snapshot_id, page.id, seo);
        } catch (e) {
          console.error("Action insert failed (non-fatal):", e?.message || e);
        }

        await heartbeat(job.id);
      }

      await setSnapshotStep(job.snapshot_id, "finalizing");
      await heartbeat(job.id);

      await supabase.rpc("scc_complete_crawl_job", { p_job_id: job.id, p_success: true });
      console.log("Completed job:", job.id);
    } catch (e) {
      console.error("Worker loop error:", e?.message || e);
      await sleep(3000);
    }
  }
}

run();
