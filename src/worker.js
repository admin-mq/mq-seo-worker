import { createClient } from "@supabase/supabase-js";
import axios from "axios";

// =====================================================
// ENV
// =====================================================

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

const WORKER_ID =
  process.env.RAILWAY_REPLICA_ID ||
  process.env.HOSTNAME ||
  `worker-${Math.random().toString(36).slice(2, 10)}`;

const POLL_INTERVAL_MS = Number(process.env.POLL_INTERVAL_MS || 5000);
const REQUEST_TIMEOUT_MS = Number(process.env.CRAWL_REQUEST_TIMEOUT_MS || 15000);
const HEARTBEAT_INTERVAL_MS = Number(process.env.HEARTBEAT_INTERVAL_MS || 10000);
const RESCUE_STALE_MINUTES = Number(process.env.RESCUE_STALE_MINUTES || 10);

const DEFAULT_MAX_PAGES = Number(process.env.CRAWL_MAX_PAGES || 25);
const DEFAULT_MAX_DEPTH = Number(process.env.CRAWL_MAX_DEPTH || 2);
const DEFAULT_CRAWL_DELAY_MS = Number(process.env.CRAWL_DELAY_MS || 700);

// =====================================================
// LOGGING
// =====================================================

function log(...args) {
  console.log(new Date().toISOString(), ...args);
}

function warn(...args) {
  console.warn(new Date().toISOString(), ...args);
}

function errorLog(...args) {
  console.error(new Date().toISOString(), ...args);
}

// =====================================================
// BASIC UTILS
// =====================================================

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
}

function safeNumber(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function normalizeWhitespace(str) {
  return String(str || "").replace(/\s+/g, " ").trim();
}

function stripHtml(html = "") {
  return normalizeWhitespace(
    String(html)
      .replace(/<script[\s\S]*?<\/script>/gi, " ")
      .replace(/<style[\s\S]*?<\/style>/gi, " ")
      .replace(/<noscript[\s\S]*?<\/noscript>/gi, " ")
      .replace(/<svg[\s\S]*?<\/svg>/gi, " ")
      .replace(/<[^>]+>/g, " ")
  );
}

function normalizeUrl(rawUrl, baseUrl = null) {
  try {
    const url = baseUrl ? new URL(rawUrl, baseUrl) : new URL(rawUrl);

    url.hash = "";
    url.protocol = url.protocol.toLowerCase();
    url.hostname = url.hostname.toLowerCase();

    if (
      (url.protocol === "http:" && url.port === "80") ||
      (url.protocol === "https:" && url.port === "443")
    ) {
      url.port = "";
    }

    url.pathname = url.pathname.replace(/\/{2,}/g, "/");

    if (url.pathname.length > 1 && url.pathname.endsWith("/")) {
      url.pathname = url.pathname.slice(0, -1);
    }

    return url.toString();
  } catch {
    return null;
  }
}

function getHostname(urlString) {
  try {
    return new URL(urlString).hostname.toLowerCase();
  } catch {
    return null;
  }
}

function isSameHost(urlA, urlB) {
  const hostA = getHostname(urlA);
  const hostB = getHostname(urlB);
  return !!hostA && !!hostB && hostA === hostB;
}

function isLikelyFileUrl(urlString) {
  try {
    const pathname = new URL(urlString).pathname.toLowerCase();
    return /\.(pdf|jpg|jpeg|png|gif|svg|webp|ico|zip|rar|7z|doc|docx|xls|xlsx|ppt|pptx|mp3|mp4|avi|mov|wmv|webm|txt|csv|xml)$/i.test(
      pathname
    );
  } catch {
    return true;
  }
}

function isCrawlableHttpUrl(urlString) {
  try {
    const u = new URL(urlString);
    if (!["http:", "https:"].includes(u.protocol)) return false;
    if (isLikelyFileUrl(urlString)) return false;
    return true;
  } catch {
    return false;
  }
}

function makeFingerprint(urlString) {
  try {
    const u = new URL(urlString);
    return `${u.origin}${u.pathname}${u.search}`;
  } catch {
    return urlString;
  }
}

function inferPageType(urlString) {
  try {
    const pathname = new URL(urlString).pathname.toLowerCase();

    if (pathname === "/" || pathname === "") return "homepage";
    if (/\/blog|\/news|\/article|\/guides|\/insights/.test(pathname)) return "content";
    if (/\/product|\/products|\/service|\/services|\/solutions|\/pricing|\/shop|\/collection/.test(pathname)) {
      return "commercial";
    }
    if (/\/contact|\/about|\/team|\/company/.test(pathname)) return "brand";
    return "standard";
  } catch {
    return "standard";
  }
}

function getBlockedReason(status) {
  if (status === 401) return "blocked_401";
  if (status === 403) return "blocked_403";
  if (status === 429) return "blocked_429";
  return null;
}

function shouldEnqueueUrl({
  normalizedUrl,
  rootUrl,
  seen,
  queued,
  nextDepth,
  maxDepth,
}) {
  if (!normalizedUrl) return false;
  if (!isCrawlableHttpUrl(normalizedUrl)) return false;
  if (!isSameHost(normalizedUrl, rootUrl)) return false;
  if (nextDepth > maxDepth) return false;

  const fp = makeFingerprint(normalizedUrl);
  if (seen.has(fp)) return false;
  if (queued.has(fp)) return false;

  return true;
}

// =====================================================
// HTML EXTRACTION
// =====================================================

function extractTitle(html) {
  const match = html.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
  return normalizeWhitespace(match?.[1] || "");
}

function extractMetaDescription(html) {
  const m1 = html.match(
    /<meta[^>]+name=["']description["'][^>]+content=["']([\s\S]*?)["'][^>]*>/i
  );
  if (m1?.[1]) return normalizeWhitespace(m1[1]);

  const m2 = html.match(
    /<meta[^>]+content=["']([\s\S]*?)["'][^>]+name=["']description["'][^>]*>/i
  );
  return normalizeWhitespace(m2?.[1] || "");
}

function extractCanonical(html, currentUrl) {
  const match = html.match(
    /<link[^>]+rel=["']canonical["'][^>]+href=["']([\s\S]*?)["'][^>]*>/i
  );
  const href = normalizeWhitespace(match?.[1] || "");
  return href ? normalizeUrl(href, currentUrl) : null;
}

function extractH1s(html) {
  return [...html.matchAll(/<h1\b[^>]*>([\s\S]*?)<\/h1>/gi)]
    .map((m) => normalizeWhitespace(stripHtml(m[1] || "")))
    .filter(Boolean);
}

function extractSchemaTypes(html) {
  const out = new Set();

  for (const match of html.matchAll(
    /<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi
  )) {
    const raw = match?.[1];
    if (!raw) continue;

    try {
      const parsed = JSON.parse(raw);

      const collectType = (node) => {
        if (!node) return;
        if (Array.isArray(node)) {
          node.forEach(collectType);
          return;
        }
        if (typeof node === "object") {
          if (node["@type"]) {
            if (Array.isArray(node["@type"])) {
              node["@type"].forEach((t) => out.add(String(t)));
            } else {
              out.add(String(node["@type"]));
            }
          }
          for (const value of Object.values(node)) {
            collectType(value);
          }
        }
      };

      collectType(parsed);
    } catch {
      // ignore invalid JSON-LD
    }
  }

  return Array.from(out);
}

function extractImageStats(html) {
  const imgTags = [...html.matchAll(/<img\b[^>]*>/gi)].map((m) => m[0] || "");
  const totalImages = imgTags.length;
  let missingAlt = 0;

  for (const tag of imgTags) {
    const alt1 = tag.match(/\balt=["']([\s\S]*?)["']/i)?.[1];
    const alt2 = tag.match(/\balt=([^\s>]+)/i)?.[1];
    const alt = normalizeWhitespace(alt1 || alt2 || "");
    if (!alt) missingAlt += 1;
  }

  return {
    totalImages,
    missingAlt,
  };
}

function extractInternalLinksFromHtml(html, currentUrl, rootUrl) {
  if (!html || typeof html !== "string") return [];

  const links = [];
  const regex = /<a\b[^>]*href\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+))/gi;

  let match;
  while ((match = regex.exec(html)) !== null) {
    const rawHref = match[1] || match[2] || match[3] || "";
    if (!rawHref) continue;

    const href = rawHref.trim();

    if (
      href.startsWith("#") ||
      href.startsWith("javascript:") ||
      href.startsWith("mailto:") ||
      href.startsWith("tel:")
    ) {
      continue;
    }

    const normalized = normalizeUrl(href, currentUrl);
    if (!normalized) continue;
    if (!isCrawlableHttpUrl(normalized)) continue;
    if (!isSameHost(normalized, rootUrl)) continue;

    links.push(normalized);
  }

  return Array.from(new Set(links));
}

// =====================================================
// FETCH
// =====================================================

async function fetchPage(url) {
  try {
    const response = await axios.get(url, {
      timeout: REQUEST_TIMEOUT_MS,
      maxRedirects: 5,
      decompress: true,
      validateStatus: () => true,
      headers: {
        "User-Agent":
          "Mozilla/5.0 (compatible; MarketersQuestSEO/1.0; +https://marketersquest.com)",
        Accept:
          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
      },
    });

    const status = response.status || null;
    const finalUrl =
      response?.request?.res?.responseUrl ||
      response?.headers?.["x-final-url"] ||
      url;
    const contentType = String(response.headers?.["content-type"] || "");
    const html = typeof response.data === "string" ? response.data : null;
    const blockedReason = getBlockedReason(status);
    const isHtml =
      contentType.toLowerCase().includes("text/html") ||
      (!!html && html.toLowerCase().includes("<html"));

    return {
      ok: status >= 200 && status < 400,
      status,
      finalUrl,
      contentType,
      html: isHtml ? html : null,
      blockedReason,
      isHtml,
      error: null,
    };
  } catch (err) {
    return {
      ok: false,
      status: null,
      finalUrl: url,
      contentType: null,
      html: null,
      blockedReason: null,
      isHtml: false,
      error: err?.message || "fetch_failed",
    };
  }
}

// =====================================================
// RPC HELPERS
// =====================================================

async function rpcOrThrow(name, args = {}) {
  const { data, error } = await supabase.rpc(name, args);
  if (error) {
    throw new Error(`${name} failed: ${error.message}`);
  }
  return data;
}

async function claimNextJob() {
  return rpcOrThrow("scc_claim_next_job", { p_worker_id: WORKER_ID });
}

async function rescueStaleJobs() {
  try {
    return await rpcOrThrow("scc_rescue_stale_jobs", {
      p_minutes: RESCUE_STALE_MINUTES,
    });
  } catch (err) {
    warn("[rescue]", err.message);
    return 0;
  }
}

async function heartbeat(jobId) {
  try {
    await rpcOrThrow("scc_job_heartbeat", {
      p_job_id: jobId,
      p_worker_id: WORKER_ID,
    });
  } catch (err) {
    warn(`[heartbeat] job=${jobId}`, err.message);
  }
}

async function completeJob(jobId, success, errorMessage = null) {
  await rpcOrThrow("scc_complete_crawl_job", {
    p_job_id: jobId,
    p_success: success,
    p_error: success ? null : String(errorMessage || "crawl failed").slice(0, 1000),
  });
}

// =====================================================
// SNAPSHOT / JOB PROGRESS UPDATES
// =====================================================

async function markSnapshotRunning(snapshotId, progressStep = "discovering") {
  const { error } = await supabase
    .from("scc_snapshots")
    .update({
      status: "running",
      progress_step: progressStep,
      finished_at: null,
      error_stage: null,
      error_message: null,
    })
    .eq("id", snapshotId);

  if (error) {
    warn(`[snapshot running] snapshot=${snapshotId}`, error.message);
  }
}

async function updateSnapshotProgress(snapshotId, progressStep) {
  const { error } = await supabase
    .from("scc_snapshots")
    .update({
      progress_step: String(progressStep || "").slice(0, 500),
    })
    .eq("id", snapshotId);

  if (error) {
    warn(`[snapshot progress] snapshot=${snapshotId}`, error.message);
  }
}

async function markSnapshotError(snapshotId, stage, message) {
  const { error } = await supabase
    .from("scc_snapshots")
    .update({
      error_stage: stage ? String(stage).slice(0, 120) : null,
      error_message: message ? String(message).slice(0, 1000) : null,
    })
    .eq("id", snapshotId);

  if (error) {
    warn(`[snapshot error] snapshot=${snapshotId}`, error.message);
  }
}

async function updateJobCounters(jobId, pagesDone, errorsCount) {
  const { error } = await supabase
    .from("scc_crawl_jobs")
    .update({
      pages_done: pagesDone,
      errors_count: errorsCount,
      worker_id: WORKER_ID,
      last_heartbeat_at: new Date().toISOString(),
    })
    .eq("id", jobId);

  if (error) {
    warn(`[job counters] job=${jobId}`, error.message);
  }
}

// =====================================================
// PAGE PERSISTENCE
// =====================================================

async function getOrCreatePage({ siteId, url, pageType }) {
  const nowIso = new Date().toISOString();

  const { data: existing, error: existingError } = await supabase
    .from("scc_pages")
    .select("id, first_seen_at")
    .eq("site_id", siteId)
    .eq("url", url)
    .maybeSingle();

  if (existingError) {
    throw new Error(`scc_pages select failed for ${url}: ${existingError.message}`);
  }

  if (existing?.id) {
    const { error: updateError } = await supabase
      .from("scc_pages")
      .update({
        page_type: pageType,
        last_seen_at: nowIso,
      })
      .eq("id", existing.id);

    if (updateError) {
      throw new Error(`scc_pages update failed for ${url}: ${updateError.message}`);
    }

    return existing.id;
  }

  const { data: inserted, error: insertError } = await supabase
    .from("scc_pages")
    .insert({
      site_id: siteId,
      url,
      page_type: pageType,
      first_seen_at: nowIso,
      last_seen_at: nowIso,
    })
    .select("id")
    .single();

  if (insertError) {
    // Safe retry for race conditions or unique collisions
    const { data: fallback, error: fallbackError } = await supabase
      .from("scc_pages")
      .select("id")
      .eq("site_id", siteId)
      .eq("url", url)
      .maybeSingle();

    if (fallbackError || !fallback?.id) {
      throw new Error(`scc_pages insert failed for ${url}: ${insertError.message}`);
    }

    return fallback.id;
  }

  return inserted.id;
}

// =====================================================
// METRICS / SCORING
// =====================================================

function getDepthModifier(depth = 0) {
  if (depth === 0) return 1.15;
  if (depth === 1) return 1.0;
  if (depth === 2) return 0.9;
  return 0.8;
}

function computePriorityBucket(score) {
  if (score >= 80) return "high";
  if (score >= 45) return "medium";
  return "low";
}

function derivePageMetrics({ url, html, status, depth }) {
  const title = extractTitle(html);
  const metaDescription = extractMetaDescription(html);
  const canonical = extractCanonical(html, url);
  const h1s = extractH1s(html);
  const schemaTypes = extractSchemaTypes(html);
  const internalLinks = extractInternalLinksFromHtml(html, url, url);
  const images = extractImageStats(html);
  const cleanText = stripHtml(html);
  const wordCount = cleanText ? cleanText.split(/\s+/).filter(Boolean).length : 0;
  const pageType = inferPageType(url);

  const hasTitle = !!title;
  const hasMeta = !!metaDescription;
  const hasH1 = h1s.length > 0;
  const canonicalOk = !!canonical;
  const indexable = status >= 200 && status < 400;

  let structuralPenalty = 0;
  let visibilityPenalty = 0;
  let revenuePenalty = 0;
  let paidRiskPenalty = 0;

  if (!indexable) {
    structuralPenalty += 60;
    visibilityPenalty += 45;
    revenuePenalty += 35;
    paidRiskPenalty += 25;
  }

  if (!hasTitle) {
    structuralPenalty += 16;
    visibilityPenalty += 14;
    revenuePenalty += 10;
  } else {
    if (title.length < 20) visibilityPenalty += 7;
    if (title.length > 65) visibilityPenalty += 5;
  }

  if (!hasMeta) {
    structuralPenalty += 9;
    visibilityPenalty += 10;
    paidRiskPenalty += 10;
  } else {
    if (metaDescription.length < 70) visibilityPenalty += 4;
    if (metaDescription.length > 165) visibilityPenalty += 4;
  }

  if (!hasH1) {
    structuralPenalty += 12;
    visibilityPenalty += 7;
    revenuePenalty += 8;
  } else if (h1s.length > 1) {
    structuralPenalty += 6;
    visibilityPenalty += 3;
  }

  if (!canonicalOk) {
    structuralPenalty += 6;
    visibilityPenalty += 8;
  }

  if (wordCount < 120) {
    visibilityPenalty += 12;
    revenuePenalty += 10;
  } else if (wordCount < 250) {
    visibilityPenalty += 6;
    revenuePenalty += 5;
  }

  if (internalLinks.length < 2) {
    structuralPenalty += 7;
    visibilityPenalty += 8;
  }

  if (images.totalImages > 0) {
    const ratio = images.missingAlt / images.totalImages;
    if (ratio >= 0.5) {
      structuralPenalty += 10;
      visibilityPenalty += 6;
    } else if (images.missingAlt > 0) {
      structuralPenalty += 4;
      visibilityPenalty += 3;
    }
  }

  if (pageType === "commercial") {
    revenuePenalty += 0;
  } else if (pageType === "homepage") {
    revenuePenalty += 4;
  } else {
    revenuePenalty += 7;
  }

  const structuralScore = clamp(100 - structuralPenalty, 0, 100);
  const visibilityScore = clamp(100 - visibilityPenalty, 0, 100);
  const revenueScore = clamp(100 - revenuePenalty, 0, 100);
  const paidRiskScore = clamp(100 - paidRiskPenalty, 0, 100);

  const rawOpportunity =
    (100 - structuralScore) * 0.3 +
    (100 - visibilityScore) * 0.35 +
    (100 - revenueScore) * 0.25 +
    (100 - paidRiskScore) * 0.1;

  const pageOpportunityScore = clamp(
    Math.round(rawOpportunity * getDepthModifier(depth)),
    0,
    100
  );

  const priorityBucket = computePriorityBucket(pageOpportunityScore);

  return {
    pageType,
    title,
    metaDescription,
    canonical,
    h1s,
    schemaTypes,
    internalLinks,
    images,
    wordCount,
    indexable,
    canonicalOk,
    hasTitle,
    hasMeta,
    hasH1,
    structuralScore,
    visibilityScore,
    revenueScore,
    paidRiskScore,
    pageOpportunityScore,
    priorityBucket,
  };
}

async function upsertSnapshotMetrics({
  snapshotId,
  pageId,
  depth,
  metrics,
}) {
  const payload = {
    snapshot_id: snapshotId,
    page_id: pageId,
    indexable: metrics.indexable,
    canonical_ok: metrics.canonicalOk,
    has_title: metrics.hasTitle,
    has_meta: metrics.hasMeta,
    has_h1: metrics.hasH1,
    schema_types: metrics.schemaTypes,
    internal_link_depth: depth,
    impressions: null,
    clicks: null,
    avg_position: null,
    ctr: null,
    sessions: null,
    conversions: null,
    revenue: null,
    paid_cost: null,
    paid_clicks: null,
    paid_conversions: null,
    paid_revenue: null,
    structural_score: metrics.structuralScore,
    visibility_score: metrics.visibilityScore,
    revenue_score: metrics.revenueScore,
    paid_risk_score: metrics.paidRiskScore,
    page_opportunity_score: metrics.pageOpportunityScore,
    priority_bucket: metrics.priorityBucket,
  };

  const { error } = await supabase
    .from("scc_page_snapshot_metrics")
    .upsert(payload, {
      onConflict: "snapshot_id,page_id",
    });

  if (error) {
    throw new Error(
      `scc_page_snapshot_metrics upsert failed for page ${pageId}: ${error.message}`
    );
  }
}

// =====================================================
// ACTIONS ENGINE
// =====================================================

function buildActions({ pageId, snapshotId, url, httpStatus, metrics }) {
  const actions = [];

  const priority = metrics.priorityBucket;
  const severity = metrics.priorityBucket;

  const pushAction = ({
    actionType,
    title,
    summary,
    whyItMatters,
    technicalReason,
    expectedImpactRange,
    steps,
    actionPriority = priority,
    actionSeverity = severity,
  }) => {
    actions.push({
      snapshot_id: snapshotId,
      page_id: pageId,
      query_id: null,
      action_type: actionType,
      summary,
      priority: actionPriority,
      status: "open",
      title,
      why_it_matters: whyItMatters,
      technical_reason: technicalReason,
      expected_impact_range: expectedImpactRange,
      steps,
      severity: actionSeverity,
    });
  };

  if (!metrics.indexable) {
    pushAction({
      actionType: "technical_fix",
      title: "Fix non-indexable page response",
      summary: "This page is not returning a healthy crawlable response.",
      whyItMatters:
        "A page that cannot be cleanly fetched is unlikely to perform in organic search and cannot reliably support SEO growth.",
      technicalReason: `The page returned an unhealthy response status (${httpStatus ?? "unknown"}).`,
      expectedImpactRange: "High",
      steps: [
        "Check whether the page returns a stable 200 response to normal browser and crawler requests.",
        "Review firewall, bot protection, redirects, and hosting rules.",
        "Ensure the canonical production URL is publicly accessible.",
      ],
      actionPriority: "high",
      actionSeverity: "high",
    });
  }

  if (!metrics.hasTitle) {
    pushAction({
      actionType: "on_page_seo",
      title: "Add an SEO title tag",
      summary: "This page is missing a title tag.",
      whyItMatters:
        "Titles are one of the clearest search relevance signals and strongly influence click-through rate.",
      technicalReason: "No <title> tag was detected in the page HTML.",
      expectedImpactRange: "Medium to High",
      steps: [
        "Write a unique title for this page.",
        "Keep it clear, specific, and aligned to the page topic.",
        "Aim for roughly 50 to 65 characters.",
      ],
    });
  }

  if (!metrics.hasMeta) {
    pushAction({
      actionType: "on_page_seo",
      title: "Add a meta description",
      summary: "This page is missing a meta description.",
      whyItMatters:
        "A strong meta description can improve click-through rate even when rankings stay the same.",
      technicalReason: "No meta description was detected in the page HTML.",
      expectedImpactRange: "Medium",
      steps: [
        "Write a concise summary of the page value.",
        "Align it to likely search intent.",
        "Aim for roughly 120 to 160 characters.",
      ],
    });
  }

  if (!metrics.hasH1) {
    pushAction({
      actionType: "content_structure",
      title: "Add a primary H1 heading",
      summary: "This page has no H1 heading.",
      whyItMatters:
        "A clear H1 helps search engines and users understand the primary topic of the page.",
      technicalReason: "No <h1> tag was detected in the page HTML.",
      expectedImpactRange: "Medium",
      steps: [
        "Add one clear H1 heading near the top of the page.",
        "Match it to the page topic and intent.",
        "Avoid duplicate or vague headings.",
      ],
    });
  } else if (metrics.h1s.length > 1) {
    pushAction({
      actionType: "content_structure",
      title: "Reduce multiple H1 headings",
      summary: "This page has more than one H1 heading.",
      whyItMatters:
        "Multiple top-level headings can weaken semantic clarity and make page structure less consistent.",
      technicalReason: `Detected ${metrics.h1s.length} H1 headings.`,
      expectedImpactRange: "Low to Medium",
      steps: [
        "Keep one primary H1 for the page.",
        "Convert secondary main headings into H2 or H3 tags.",
        "Make sure the remaining H1 reflects the page’s main intent.",
      ],
      actionPriority: "medium",
      actionSeverity: "medium",
    });
  }

  if (!metrics.canonicalOk) {
    pushAction({
      actionType: "technical_seo",
      title: "Add a canonical tag",
      summary: "This page is missing a canonical URL.",
      whyItMatters:
        "Canonical signals help search engines understand the preferred version of a page and reduce duplication risk.",
      technicalReason: "No rel=canonical link tag was detected.",
      expectedImpactRange: "Low to Medium",
      steps: [
        "Add a canonical tag in the page head.",
        "Point it to the preferred final page URL.",
        "Make sure the canonical URL is indexable and returns 200.",
      ],
      actionPriority: "medium",
      actionSeverity: "medium",
    });
  }

  if (metrics.wordCount < 120) {
    pushAction({
      actionType: "content_improvement",
      title: "Expand thin page content",
      summary: "This page appears thin relative to ranking and conversion potential.",
      whyItMatters:
        "Thin pages often struggle to rank well and may fail to answer user intent clearly enough to convert.",
      technicalReason: `Estimated visible text word count is ${metrics.wordCount}.`,
      expectedImpactRange: "Medium to High",
      steps: [
        "Add more useful, unique content aligned to the page topic.",
        "Clarify the offer, product, service, or information intent.",
        "Include supporting subheadings, proof points, and FAQs where relevant.",
      ],
    });
  }

  if (metrics.internalLinks.length < 2) {
    pushAction({
      actionType: "internal_linking",
      title: "Improve internal linking into and out of this page",
      summary: "This page has very limited internal linking signals.",
      whyItMatters:
        "Internal links help discovery, strengthen topical relationships, and distribute authority through the site.",
      technicalReason: `Detected only ${metrics.internalLinks.length} same-host links on the page.`,
      expectedImpactRange: "Medium",
      steps: [
        "Link this page from relevant nearby pages.",
        "Add contextual internal links from this page to related pages.",
        "Use descriptive anchor text that reflects the linked topic.",
      ],
      actionPriority: "medium",
      actionSeverity: "medium",
    });
  }

  if (metrics.images.totalImages > 0 && metrics.images.missingAlt > 0) {
    pushAction({
      actionType: "accessibility_seo",
      title: "Add missing image alt text",
      summary: "Some images on this page are missing alt text.",
      whyItMatters:
        "Alt text improves accessibility and can strengthen image relevance signals.",
      technicalReason: `${metrics.images.missingAlt} of ${metrics.images.totalImages} images appear to be missing alt text.`,
      expectedImpactRange: "Low",
      steps: [
        "Add descriptive alt text to meaningful images.",
        "Keep alt text concise and specific.",
        "Use empty alt text only for decorative images.",
      ],
      actionPriority: "low",
      actionSeverity: "low",
    });
  }

  return actions;
}

async function replaceActionsForPage(snapshotId, pageId, actions) {
  const { error: deleteError } = await supabase
    .from("scc_actions")
    .delete()
    .eq("snapshot_id", snapshotId)
    .eq("page_id", pageId);

  if (deleteError) {
    throw new Error(
      `scc_actions delete failed for page ${pageId}: ${deleteError.message}`
    );
  }

  if (!actions.length) return;

  const { error: insertError } = await supabase
    .from("scc_actions")
    .insert(actions);

  if (insertError) {
    throw new Error(
      `scc_actions insert failed for page ${pageId}: ${insertError.message}`
    );
  }
}

// =====================================================
// PER-PAGE PROCESSING
// =====================================================

async function processPage({
  siteId,
  snapshotId,
  pageUrl,
  finalUrl,
  status,
  depth,
  html,
}) {
  const normalizedFinalUrl = normalizeUrl(finalUrl || pageUrl) || normalizeUrl(pageUrl);

  if (!normalizedFinalUrl) {
    throw new Error(`Invalid final URL for page: ${pageUrl}`);
  }

  const pageType = inferPageType(normalizedFinalUrl);
  const pageId = await getOrCreatePage({
    siteId,
    url: normalizedFinalUrl,
    pageType,
  });

  const metrics = derivePageMetrics({
    url: normalizedFinalUrl,
    html,
    status,
    depth,
  });

  await upsertSnapshotMetrics({
    snapshotId,
    pageId,
    depth,
    metrics,
  });

  const actions = buildActions({
    pageId,
    snapshotId,
    url: normalizedFinalUrl,
    httpStatus: status,
    metrics,
  });

  await replaceActionsForPage(snapshotId, pageId, actions);

  return {
    pageId,
    finalUrl: normalizedFinalUrl,
    metrics,
  };
}

// =====================================================
// MULTI-PAGE CRAWL ENGINE
// =====================================================

async function runMultiPageCrawl(job) {
  const jobId = job.id;
  const siteId = job.site_id;
  const snapshotId = job.snapshot_id;
  const rootUrl = normalizeUrl(job.seed_url);

  if (!rootUrl) {
    throw new Error(`Invalid seed_url: ${job.seed_url}`);
  }

  const maxPages = safeNumber(job.max_pages, DEFAULT_MAX_PAGES);
  const maxDepth = safeNumber(job.max_depth, DEFAULT_MAX_DEPTH);
  const crawlDelayMs = safeNumber(job.crawl_delay_ms, DEFAULT_CRAWL_DELAY_MS);

  const queue = [{ url: rootUrl, depth: 0 }];
  const queued = new Set([makeFingerprint(rootUrl)]);
  const seen = new Set();

  const visitedPages = [];
  const blockedPages = [];
  const failedPages = [];

  let pagesDone = 0;
  let errorsCount = 0;
  let lastHeartbeatAt = 0;

  await markSnapshotRunning(snapshotId, "discovering");

  while (queue.length > 0 && pagesDone < maxPages) {
    const current = queue.shift();
    const currentUrl = current.url;
    const currentDepth = current.depth;
    const fp = makeFingerprint(currentUrl);

    if (seen.has(fp)) continue;
    seen.add(fp);

    const now = Date.now();
    if (now - lastHeartbeatAt >= HEARTBEAT_INTERVAL_MS) {
      await heartbeat(jobId);
      lastHeartbeatAt = now;
    }

    await updateSnapshotProgress(
      snapshotId,
      `crawling ${pagesDone + 1}/${maxPages} | depth ${currentDepth} | ${currentUrl.slice(0, 180)}`
    );

    log(
      `[crawl] job=${jobId} snapshot=${snapshotId} depth=${currentDepth} fetching=${currentUrl}`
    );

    const fetched = await fetchPage(currentUrl);

    log(
      `[crawl] fetched url=${currentUrl} status=${fetched.status} final=${fetched.finalUrl} type=${fetched.contentType} html=${!!fetched.html}`
    );

    if (fetched.blockedReason) {
      errorsCount += 1;
      blockedPages.push({
        url: currentUrl,
        status: fetched.status,
        reason: fetched.blockedReason,
      });

      await updateJobCounters(jobId, pagesDone, errorsCount);

      // If homepage itself is blocked, fail the whole job
      if (pagesDone === 0 && currentDepth === 0) {
        throw new Error(
          `Crawl blocked at seed URL: ${fetched.blockedReason} (${fetched.status})`
        );
      }

      continue;
    }

    if (!fetched.ok || !fetched.isHtml || !fetched.html) {
      errorsCount += 1;
      failedPages.push({
        url: currentUrl,
        status: fetched.status,
        error: fetched.error || "non_html_or_fetch_failed",
      });

      await updateJobCounters(jobId, pagesDone, errorsCount);

      if (pagesDone === 0 && currentDepth === 0) {
        throw new Error(
          `Seed URL is not crawlable HTML: ${fetched.error || fetched.status || "unknown_error"}`
        );
      }

      continue;
    }

    const processed = await processPage({
      siteId,
      snapshotId,
      pageUrl: currentUrl,
      finalUrl: fetched.finalUrl,
      status: fetched.status,
      depth: currentDepth,
      html: fetched.html,
    });

    pagesDone += 1;
    visitedPages.push({
      url: processed.finalUrl,
      pageId: processed.pageId,
      depth: currentDepth,
      priorityBucket: processed.metrics.priorityBucket,
      pageOpportunityScore: processed.metrics.pageOpportunityScore,
    });

    await updateJobCounters(jobId, pagesDone, errorsCount);

    if (currentDepth < maxDepth && pagesDone < maxPages) {
      const links = extractInternalLinksFromHtml(
        fetched.html,
        processed.finalUrl,
        rootUrl
      );

      for (const link of links) {
        const nextDepth = currentDepth + 1;
        if (
          shouldEnqueueUrl({
            normalizedUrl: link,
            rootUrl,
            seen,
            queued,
            nextDepth,
            maxDepth,
          })
        ) {
          queue.push({ url: link, depth: nextDepth });
          queued.add(makeFingerprint(link));
        }
      }
    }

    if (crawlDelayMs > 0) {
      await sleep(crawlDelayMs);
    }
  }

  return {
    rootUrl,
    pagesDone,
    errorsCount,
    visitedPages,
    blockedPages,
    failedPages,
  };
}

// =====================================================
// JOB PROCESSING
// =====================================================

async function processClaimedJob(rawJob) {
  const job = rawJob;

  const jobId = job?.id;
  const snapshotId = job?.snapshot_id;
  const siteId = job?.site_id;
  const seedUrl = job?.seed_url;

  if (!jobId) throw new Error("Claimed job missing id");
  if (!snapshotId) throw new Error(`Claimed job ${jobId} missing snapshot_id`);
  if (!siteId) throw new Error(`Claimed job ${jobId} missing site_id`);
  if (!seedUrl) throw new Error(`Claimed job ${jobId} missing seed_url`);

  log(
    `[job] start id=${jobId} snapshot=${snapshotId} site=${siteId} seed=${seedUrl} max_pages=${job.max_pages} max_depth=${job.max_depth}`
  );

  // Clean snapshot rows for safe retry on the same snapshot/job
  {
    const { error: metricsDeleteError } = await supabase
      .from("scc_page_snapshot_metrics")
      .delete()
      .eq("snapshot_id", snapshotId);

    if (metricsDeleteError) {
      warn(`[cleanup metrics] snapshot=${snapshotId}`, metricsDeleteError.message);
    }

    const { error: actionsDeleteError } = await supabase
      .from("scc_actions")
      .delete()
      .eq("snapshot_id", snapshotId);

    if (actionsDeleteError) {
      warn(`[cleanup actions] snapshot=${snapshotId}`, actionsDeleteError.message);
    }
  }

  try {
    const result = await runMultiPageCrawl(job);

    if (result.pagesDone === 0) {
      throw new Error("No crawlable HTML pages found");
    }

    await updateSnapshotProgress(
      snapshotId,
      `done | pages ${result.pagesDone} | errors ${result.errorsCount}`
    );

    await completeJob(jobId, true, null);

    log(
      `[job] complete id=${jobId} snapshot=${snapshotId} pages=${result.pagesDone} errors=${result.errorsCount}`
    );
  } catch (err) {
    const message = err?.message || "crawl failed";
    await markSnapshotError(snapshotId, "crawl", message);
    await completeJob(jobId, false, message);
    throw err;
  }
}

// =====================================================
// MAIN LOOP
// =====================================================

async function workerLoop() {
  log(`[worker] online worker_id=${WORKER_ID}`);

  while (true) {
    try {
      await rescueStaleJobs();

      const claimed = await claimNextJob();

      if (!claimed) {
        await sleep(POLL_INTERVAL_MS);
        continue;
      }

      try {
        await processClaimedJob(claimed);
      } catch (err) {
        errorLog("[job failed]", err?.message || err);
      }
    } catch (err) {
      errorLog("[worker loop error]", err?.message || err);
      await sleep(POLL_INTERVAL_MS);
    }
  }
}

// =====================================================
// START
// =====================================================

workerLoop().catch((err) => {
  errorLog("[fatal]", err?.message || err);
  process.exit(1);
});
