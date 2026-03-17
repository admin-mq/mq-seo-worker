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

const HEARTBEAT_EVERY_MS = 15000;
let lastHeartbeatAt = 0;

async function heartbeat(jobId) {
  if (!jobId) return;

  const now = Date.now();
  if (now - lastHeartbeatAt < HEARTBEAT_EVERY_MS) return;
  lastHeartbeatAt = now;

  const { error } = await supabase.rpc("scc_job_heartbeat", {
    p_job_id: jobId,
    p_worker_id: WORKER_ID,
  });

  if (error) {
    console.error("Heartbeat error:", error.message || error);
  }
}

function normalizeUrl(url) {
  if (typeof url !== "string") return null;

  let raw = url.trim();
  if (!raw) return null;

  if (!/^https?:\/\//i.test(raw)) {
    raw = `https://${raw}`;
  }

  try {
    const u = new URL(raw);
    u.hash = "";

    ["gclid", "fbclid"].forEach((p) => u.searchParams.delete(p));
    [...u.searchParams.keys()].forEach((k) => {
      if (k.toLowerCase().startsWith("utm_")) u.searchParams.delete(k);
    });

    u.hostname = u.hostname.toLowerCase();

    if (u.pathname !== "/" && u.pathname.endsWith("/")) {
      u.pathname = u.pathname.slice(0, -1);
    }

    return u.toString();
  } catch {
    return raw;
  }
}

function stripTags(s) {
  return String(s || "")
    .replace(/<[^>]*>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function extractSeo(html, finalUrl) {
  const safeHtml = String(html || "");

  const titleMatch = safeHtml.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
  const titleText = titleMatch ? stripTags(titleMatch[1]) : "";
  const hasTitle = titleText.length > 0;

  const metaMatch = safeHtml.match(
    /<meta\s+[^>]*name=["']description["'][^>]*content=["']([^"']*)["'][^>]*>/i
  );
  const hasMeta = !!(metaMatch && metaMatch[1].trim().length > 0);

  const h1Matches = safeHtml.match(/<h1\b[^>]*>/gi);
  const hasH1 = (h1Matches ? h1Matches.length : 0) > 0;

  const robotsMatch = safeHtml.match(
    /<meta\s+[^>]*name=["']robots["'][^>]*content=["']([^"']*)["'][^>]*>/i
  );
  const robotsContent = robotsMatch ? robotsMatch[1].toLowerCase() : "";
  const indexable = !robotsContent.includes("noindex");

  const canonMatch = safeHtml.match(
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

  const schemaTypes = [];
  const jsonLdMatches = [
    ...safeHtml.matchAll(
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
        else if (Array.isArray(t)) {
          schemaTypes.push(...t.filter((x) => typeof x === "string"));
        }
      }
    } catch {
      // ignore invalid JSON-LD
    }
  }

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

async function setSnapshotState(snapshotId, status, step) {
  if (!snapshotId) {
    console.error("setSnapshotState skipped: missing snapshotId");
    return;
  }

  const payload = {};
  if (status) payload.status = status;
  if (step) payload.progress_step = step;
  if (status === "running") payload.finished_at = null;
  if (status === "success" || status === "failed") {
    payload.finished_at = new Date().toISOString();
  }

  const { error } = await supabase.from("scc_snapshots").update(payload).eq("id", snapshotId);

  if (error) {
    console.error("setSnapshotState error:", error.message || error);
  }
}

async function setSnapshotStep(snapshotId, step) {
  if (!snapshotId) {
    console.error("setSnapshotStep skipped: missing snapshotId");
    return;
  }

  const { error } = await supabase
    .from("scc_snapshots")
    .update({ progress_step: step })
    .eq("id", snapshotId);

  if (error) {
    console.error("setSnapshotStep error:", error.message || error);
  }
}

async function upsertPage(siteId, url) {
  const { data, error } = await supabase
    .from("scc_pages")
    .upsert({ site_id: siteId, url }, { onConflict: "site_id,url" })
    .select("id, url")
    .maybeSingle();

  if (error) throw error;
  return data;
}

async function upsertPageMetrics(snapshotId, pageId, seo, depth) {
  const payload = {
    snapshot_id: snapshotId,
    page_id: pageId,
    indexable: seo.indexable,
    canonical_ok: seo.canonicalOk,
    has_title: seo.hasTitle,
    has_meta: seo.hasMeta,
    has_h1: seo.hasH1,
    schema_types: seo.schemaTypes,
    internal_link_depth: depth,
    structural_score: seo.structuralScore,
  };

  const { error } = await supabase
    .from("scc_page_snapshot_metrics")
    .upsert(payload, { onConflict: "snapshot_id,page_id" });

  if (error) throw error;
}

async function insertBasicActions(snapshotId, pageId, seo) {
  const actions = [];

  if (!seo.hasTitle) {
    actions.push({
      snapshot_id: snapshotId,
      page_id: pageId,
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
      snapshot_id: snapshotId,
      page_id: pageId,
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
      snapshot_id: snapshotId,
      page_id: pageId,
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
      timeout: 20000,
      maxRedirects: 5,
      headers: {
        "user-agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        accept:
          "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "accept-language": "en-US,en;q=0.9",
        "cache-control": "no-cache",
        pragma: "no-cache",
        "upgrade-insecure-requests": "1",
        referer: url,
      },
      validateStatus: () => true,
    });

    const contentType = (res.headers?.["content-type"] || "").toLowerCase();
    const finalUrl = (res.request?.res && res.request.res.responseUrl) || url;
    const html = typeof res.data === "string" ? res.data : null;

    if ([401, 403, 429].includes(res.status)) {
      return {
        ok: false,
        blocked: true,
        status: res.status,
        contentType,
        finalUrl,
        html,
        error: `blocked with status ${res.status}`,
      };
    }

    if (!contentType.includes("text/html")) {
      return {
        ok: false,
        blocked: false,
        status: res.status,
        contentType,
        finalUrl,
        html: null,
        error: `non-html response (${contentType || "unknown"})`,
      };
    }

    if (res.status < 200 || res.status >= 400) {
      return {
        ok: false,
        blocked: false,
        status: res.status,
        contentType,
        finalUrl,
        html,
        error: `http status ${res.status}`,
      };
    }

    return {
      ok: true,
      blocked: false,
      status: res.status,
      contentType,
      finalUrl,
      html,
    };
  } catch (err) {
    const msg =
      err?.code === "ECONNABORTED"
        ? "timeout after 20s"
        : err?.message || String(err);

    return {
      ok: false,
      blocked: false,
      status: null,
      contentType: null,
      finalUrl: url,
      html: null,
      error: msg,
    };
  }
}

async function rescueStaleJobs() {
  const { data, error } = await supabase.rpc("scc_rescue_stale_jobs", {
    p_minutes: 10,
  });

  if (error) {
    console.error("Rescue stale jobs error:", error.message || error);
    return;
  }

  if (data && data > 0) {
    console.log("Rescued stale jobs:", data);
  }
}

async function claimNextJob() {
  const { data, error } = await supabase.rpc("scc_claim_next_job", {
    p_worker_id: WORKER_ID,
  });

  if (error) throw error;
  if (!data) return null;

  if (!data.id || !data.snapshot_id || !data.site_id || !data.seed_url) {
    console.error("Malformed claimed job payload:", data);
    return null;
  }

  return data;
}

async function failJob(jobId, message) {
  if (!jobId) return;

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

      if (loopCount % 30 === 1) {
        await rescueStaleJobs();
      }

      const job = await claimNextJob();

      if (!job) {
        await sleep(2000);
        continue;
      }

      if (!job.id || !job.snapshot_id || !job.site_id || !job.seed_url) {
        console.error("Skipping malformed job:", job);
        await sleep(2000);
        continue;
      }

      lastHeartbeatAt = 0;
      console.log("Picked job:", job.id, "snapshot:", job.snapshot_id);

      await heartbeat(job.id);

      await setSnapshotState(job.snapshot_id, "running", "discovering");
      await heartbeat(job.id);

      const seed = normalizeUrl(job.seed_url);
      if (!seed) {
        await failJob(job.id, "Missing or invalid seed_url");
        continue;
      }

      const { error: enqueueErr } = await supabase.rpc("scc_enqueue_urls", {
        p_job_id: job.id,
        p_site_id: job.site_id,
        p_snapshot_id: job.snapshot_id,
        p_urls: [seed],
        p_url_normalized: [seed],
        p_depth: 0,
      });

      if (enqueueErr) throw enqueueErr;

      await heartbeat(job.id);

      await setSnapshotStep(job.snapshot_id, "analyzing");
      await heartbeat(job.id);

      const { data: q, error: claimErr } = await supabase.rpc("scc_claim_next_url", {
        p_job_id: job.id,
        p_worker_id: WORKER_ID,
        p_lock_minutes: 10,
      });

      if (claimErr) throw claimErr;

      if (!q || !q.id || !q.url) {
        console.log("No valid queue row claimed for job:", job.id, q);
        await supabase.rpc("scc_complete_crawl_job", {
          p_job_id: job.id,
          p_success: true,
        });
        console.log("Completed job (no urls):", job.id);
        continue;
      }

      await heartbeat(job.id);

      const fetched = await fetchHtml(q.url);

      console.log(
        "Fetched URL:",
        q.url,
        "status:",
        fetched.status,
        "contentType:",
        fetched.contentType,
        "finalUrl:",
        fetched.finalUrl,
        "hasHtml:",
        !!fetched.html
      );

      if (!fetched.ok) {
        console.error(
          "Fetch failed for:",
          q.url,
          "reason:",
          fetched.error,
          "status:",
          fetched.status,
          "contentType:",
          fetched.contentType,
          "blocked:",
          !!fetched.blocked
        );

        await supabase.rpc("scc_mark_url_result", {
          p_queue_id: q.id,
          p_success: false,
          p_http_status: fetched.status,
          p_content_type: fetched.contentType,
          p_final_url: fetched.finalUrl || q.url,
          p_canonical_url: null,
          p_error: fetched.error || "fetch failed",
        });

        const failMessage = fetched.blocked
          ? `Site blocked crawler request (${fetched.status})`
          : `Fetch failed: ${fetched.error || "unknown"}`;

        await failJob(job.id, failMessage);
        continue;
      }

      await heartbeat(job.id);

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

      const finalNormalizedUrl = normalizeUrl(fetched.finalUrl || q.url);
      if (!finalNormalizedUrl) {
        throw new Error("Fetched URL could not be normalized");
      }

      if (!fetched.html) {
        console.error(
          "No HTML body extracted for:",
          q.url,
          "status:",
          fetched.status,
          "contentType:",
          fetched.contentType,
          "finalUrl:",
          fetched.finalUrl
        );

        await failJob(
          job.id,
          `No crawlable HTML returned (status=${fetched.status || "unknown"}, contentType=${
            fetched.contentType || "unknown"
          })`
        );
        continue;
      }

      const seo = extractSeo(fetched.html, fetched.finalUrl || q.url);

      const page = await upsertPage(job.site_id, finalNormalizedUrl);
      await heartbeat(job.id);

      await upsertPageMetrics(job.snapshot_id, page.id, seo, q.depth ?? 0);
      console.log("Inserted page metrics for snapshot:", job.snapshot_id, "page:", page.id);
      await heartbeat(job.id);

      try {
        await insertBasicActions(job.snapshot_id, page.id, seo);
        console.log("Inserted basic actions for snapshot:", job.snapshot_id, "page:", page.id);
      } catch (e) {
        console.error("Action insert failed (non-fatal):", e?.message || e);
      }

      await heartbeat(job.id);

      await setSnapshotStep(job.snapshot_id, "finalizing");
      await heartbeat(job.id);

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
