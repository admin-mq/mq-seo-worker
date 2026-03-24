import { createClient } from "@supabase/supabase-js";
import axios from "axios";
import * as cheerio from "cheerio";
import { CookieJar } from "tough-cookie";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

const WORKER_ID =
  process.env.WORKER_ID ||
  process.env.RAILWAY_REPLICA_ID ||
  `worker-${Math.random().toString(36).slice(2, 10)}`;

const POLL_MS = Number(process.env.POLL_MS || 4000);
const HEARTBEAT_MS = Number(process.env.HEARTBEAT_MS || 15000);
const REQUEST_TIMEOUT_MS = Number(process.env.REQUEST_TIMEOUT_MS || 12000);
const RESCUE_STALE_AFTER_MIN = Number(process.env.RESCUE_STALE_AFTER_MIN || 10);
const MAX_REDIRECTS = Number(process.env.MAX_REDIRECTS || 5);

const NON_HTML_EXTENSIONS = [
  ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico",
  ".pdf", ".zip", ".rar", ".7z", ".mp4", ".mp3", ".mov", ".avi",
  ".woff", ".woff2", ".ttf", ".eot", ".css", ".js", ".xml", ".json",
  ".txt", ".csv", ".xlsx", ".doc", ".docx", ".ppt", ".pptx",
];

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function nowIso() {
  return new Date().toISOString();
}

function safeUrl(input, base = null) {
  try {
    return base ? new URL(input, base) : new URL(input);
  } catch {
    return null;
  }
}

function stripWww(hostname) {
  return hostname.replace(/^www\./i, "").toLowerCase();
}

function sameHost(a, b) {
  const ua = safeUrl(a);
  const ub = safeUrl(b);
  if (!ua || !ub) return false;
  return stripWww(ua.hostname) === stripWww(ub.hostname);
}

function normalizeUrl(rawUrl) {
  const url = safeUrl(rawUrl);
  if (!url) return null;

  url.hash = "";
  url.hostname = url.hostname.toLowerCase();

  if (
    (url.protocol === "https:" && url.port === "443") ||
    (url.protocol === "http:" && url.port === "80")
  ) {
    url.port = "";
  }

  const removableParams = [
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "gclid",
    "fbclid",
    "msclkid",
    "mc_cid",
    "mc_eid",
    "_hsenc",
    "_hsmi",
  ];

  removableParams.forEach((param) => url.searchParams.delete(param));

  if (url.pathname !== "/" && url.pathname.endsWith("/")) {
    url.pathname = url.pathname.slice(0, -1);
  }

  return url.toString();
}

function isLikelyHtmlUrl(urlString) {
  const url = safeUrl(urlString);
  if (!url) return false;
  const path = url.pathname.toLowerCase();
  return !NON_HTML_EXTENSIONS.some((ext) => path.endsWith(ext));
}

function cleanText(text = "") {
  return text.replace(/\s+/g, " ").trim();
}

function countWords(text = "") {
  const cleaned = cleanText(text);
  if (!cleaned) return 0;
  return cleaned.split(/\s+/).length;
}

function clamp(num, min, max) {
  return Math.max(min, Math.min(max, num));
}

function avg(nums = []) {
  if (!nums.length) return 0;
  return Math.round(nums.reduce((a, b) => a + b, 0) / nums.length);
}

function getPathSegments(urlString) {
  const url = safeUrl(urlString);
  if (!url) return [];
  return url.pathname.split("/").filter(Boolean).map((s) => s.toLowerCase());
}

function pathDepth(urlString) {
  return getPathSegments(urlString).length;
}

function isLikelyDateSegment(segment) {
  return /^\d{4}$/.test(segment) || /^(0?[1-9]|1[0-2])$/.test(segment);
}

function scoreSlugHint(urlString) {
  const url = safeUrl(urlString);
  if (!url) return 0;

  const path = url.pathname.toLowerCase();
  let score = 0;

  if (path === "/" || path === "") score += 50;
  if (/pricing|plans|plan/.test(path)) score += 24;
  if (/service|services/.test(path)) score += 28;
  if (/product|products/.test(path)) score += 28;
  if (/solution|solutions/.test(path)) score += 24;
  if (/category|categories|collections|collection|shop|store/.test(path)) score += 16;
  if (/about|company|why-us/.test(path)) score += 8;
  if (/contact|book|demo|get-started|trial/.test(path)) score += 14;
  if (/blog|news|article|articles|post|posts|insights/.test(path)) score -= 4;
  if (/privacy|cookie|terms|policy|legal|refund|shipping/.test(path)) score -= 28;
  if (/cart|checkout|account|login|signin|search/.test(path)) score -= 40;

  return score;
}

function isArticleLikeUrl(urlString) {
  const url = safeUrl(urlString);
  if (!url) return false;

  const path = url.pathname.toLowerCase();
  const segs = getPathSegments(urlString);

  if (segs.length >= 2 && segs.some((s) => /^\d{4}$/.test(s))) return true;
  if (/\/\d{4}\/\d{1,2}\//.test(path)) return true;
  if (/\/blog\//.test(path) && segs.length >= 2) return true;
  if (/\/article\//.test(path) || /\/articles\//.test(path)) return true;
  if (/\/post\//.test(path) || /\/posts\//.test(path)) return true;
  if (/\/news\//.test(path) && segs.length >= 2) return true;
  if (/\/guides\//.test(path) && segs.length >= 2) return true;
  if (/\/resources\//.test(path) && segs.length >= 2) return true;
  if (segs.length >= 1 && segs[segs.length - 1].split("-").length >= 3) {
    const weakArchiveTerms = new Set([
      "category",
      "categories",
      "blog",
      "blogs",
      "news",
      "tag",
      "tags",
      "author",
      "page",
      "topics",
    ]);
    if (!weakArchiveTerms.has(segs[0])) return true;
  }

  return false;
}

function isArchiveLikeUrl(urlString) {
  const url = safeUrl(urlString);
  if (!url) return false;

  const path = url.pathname.toLowerCase();
  const segs = getPathSegments(urlString);

  if (/\/category\//.test(path)) return true;
  if (/\/tag\//.test(path)) return true;
  if (/\/author\//.test(path)) return true;
  if (/\/page\/\d+/.test(path)) return true;
  if (/\/blog$/.test(path) || /\/blog\/?$/.test(path)) return true;
  if (/\/articles$/.test(path) || /\/posts$/.test(path)) return true;
  if (/\/news$/.test(path) || /\/resources$/.test(path) || /\/guides$/.test(path)) return true;
  if (segs.length === 1 && ["blog", "news", "resources", "guides", "articles", "topics"].includes(segs[0])) {
    return true;
  }

  return false;
}

function detectSchemaTypes($) {
  const types = new Set();

  $("[itemscope][itemtype]").each((_, el) => {
    const itemType = ($(el).attr("itemtype") || "").trim();
    if (!itemType) return;
    const shortType = itemType.split("/").pop();
    if (shortType) types.add(shortType);
  });

  $('script[type="application/ld+json"]').each((_, el) => {
    const raw = $(el).html() || "";
    const matches = raw.match(/"@type"\s*:\s*"([^"]+)"/g) || [];
    for (const match of matches) {
      const typeMatch = match.match(/"@type"\s*:\s*"([^"]+)"/);
      if (typeMatch?.[1]) {
        types.add(typeMatch[1]);
      }
    }
  });

  return Array.from(types).slice(0, 20);
}

function addScore(map, key, amount) {
  map[key] = (map[key] || 0) + amount;
}

function bestScoredType(scoreMap, fallback = "general") {
  let bestType = fallback;
  let bestScore = -Infinity;

  for (const [type, score] of Object.entries(scoreMap)) {
    if (score > bestScore) {
      bestScore = score;
      bestType = type;
    }
  }

  return { type: bestType, score: bestScore };
}

function classifyPageTypeFromSignals({
  url,
  anchorText = "",
  title = "",
  h1Text = "",
  bodyText = "",
  schemaTypes = [],
}) {
  const normalizedUrl = normalizeUrl(url) || url;
  const urlObj = safeUrl(normalizedUrl);
  const path = (urlObj?.pathname || "").toLowerCase();
  const segs = getPathSegments(normalizedUrl);
  const slug = segs[segs.length - 1] || "";
  const anchor = cleanText(anchorText).toLowerCase();
  const titleText = cleanText(title).toLowerCase();
  const h1 = cleanText(h1Text).toLowerCase();
  const combined = `${titleText} ${h1} ${anchor}`.toLowerCase();
  const body = cleanText(bodyText).toLowerCase();
  const schema = (schemaTypes || []).map((s) => String(s).toLowerCase());

  if (path === "/" || path === "") return "homepage";

  const scores = {
    homepage: 0,
    article: 0,
    archive: 0,
    category: 0,
    product: 0,
    service: 0,
    pricing: 0,
    conversion: 0,
    policy: 0,
    contact: 0,
    about: 0,
    location: 0,
    feature: 0,
    proof: 0,
    case_study: 0,
    general: 0,
  };

  addScore(scores, "general", 10);

  if (/privacy|cookie|terms|policy|legal|refund|return-policy|shipping-policy|disclaimer/.test(path)) {
    addScore(scores, "policy", 120);
  }
  if (/contact|support|help|customer-service/.test(path)) addScore(scores, "contact", 90);
  if (/about|company|our-story|who-we-are|team|leadership/.test(path)) addScore(scores, "about", 80);
  if (/pricing|plans/.test(path)) addScore(scores, "pricing", 95);
  if (/demo|book-demo|get-started|start-now|free-trial|trial|contact-sales/.test(path)) {
    addScore(scores, "conversion", 95);
  }
  if (/location|locations|city|area|near-me/.test(path)) addScore(scores, "location", 75);
  if (/case-study|case-studies|success-story|success-stories/.test(path)) addScore(scores, "case_study", 90);
  if (/testimonial|testimonials|review|reviews/.test(path)) addScore(scores, "proof", 80);
  if (/feature|features|capabilities|platform|technology/.test(path)) addScore(scores, "feature", 70);
  if (/service|services|solution|solutions/.test(path)) addScore(scores, "service", 72);
  if (/product|products|sku|item|\/p\/|\/pdp\/|\/product\//.test(path)) addScore(scores, "product", 85);
  if (/category|categories/.test(path)) addScore(scores, "category", 90);
  if (/collection|collections|shop|store|catalog|browse/.test(path)) addScore(scores, "category", 68);
  if (isArchiveLikeUrl(normalizedUrl)) addScore(scores, "archive", 100);
  if (isArticleLikeUrl(normalizedUrl)) addScore(scores, "article", 88);

  if (segs.length >= 2 && segs.some(isLikelyDateSegment)) {
    addScore(scores, "article", 25);
    addScore(scores, "archive", -8);
  }

  if (schema.some((s) => ["article", "blogposting", "newsarticle", "medicalwebpage", "howto"].includes(s))) {
    addScore(scores, "article", 30);
  }
  if (schema.some((s) => ["product"].includes(s))) addScore(scores, "product", 30);
  if (schema.some((s) => ["faqpage", "softwareapplication", "service"].includes(s))) {
    addScore(scores, "service", 14);
    addScore(scores, "feature", 10);
  }

  if (/price|pricing|plans|cost/.test(combined)) addScore(scores, "pricing", 26);
  if (/book demo|request demo|get started|free trial|start free|contact sales|enquire now/.test(combined)) {
    addScore(scores, "conversion", 28);
  }
  if (/service|services|solution|solutions/.test(combined)) addScore(scores, "service", 18);
  if (/case study|success story/.test(combined)) addScore(scores, "case_study", 24);
  if (/testimonial|review|client story/.test(combined)) addScore(scores, "proof", 18);
  if (/about us|our company|our team/.test(combined)) addScore(scores, "about", 22);
  if (/contact us|support/.test(combined)) addScore(scores, "contact", 22);
  if (/category|categories|browse|archive|archives/.test(combined)) {
    addScore(scores, "archive", 22);
    addScore(scores, "category", 16);
  }
  if (/blog|blogs|news|article|articles|post|posts|insights|guides|resources/.test(combined)) {
    addScore(scores, "article", 14);
    addScore(scores, "archive", 14);
  }

  if (/posted on|published on|written by|author|leave a comment|read more/.test(body)) {
    addScore(scores, "article", 18);
  }
  if (/category archives|tag archives|author archives/.test(body)) addScore(scores, "archive", 25);

  if (pathDepth(normalizedUrl) >= 2 && slug.split("-").length >= 3) addScore(scores, "article", 10);
  if (pathDepth(normalizedUrl) === 1 && slug.split("-").length <= 2) {
    addScore(scores, "service", 4);
    addScore(scores, "pricing", 4);
    addScore(scores, "feature", 3);
  }

  const articleBias =
    isArticleLikeUrl(normalizedUrl) ||
    schema.some((s) => ["article", "blogposting", "newsarticle", "howto"].includes(s)) ||
    /posted on|published on|written by|author/.test(body);

  if (articleBias) {
    addScore(scores, "article", 25);
    addScore(scores, "pricing", -30);
    addScore(scores, "conversion", -26);
    addScore(scores, "service", -16);
    addScore(scores, "feature", -10);
  }

  const archiveBias = isArchiveLikeUrl(normalizedUrl) || /category archives|tag archives|author archives/.test(body);
  if (archiveBias) {
    addScore(scores, "archive", 20);
    addScore(scores, "pricing", -22);
    addScore(scores, "conversion", -18);
    addScore(scores, "service", -12);
  }

  const strongCommercialIntent =
    /pricing|plans|book demo|get started|free trial|request demo|contact sales/.test(combined) ||
    /pricing|plans|demo|trial/.test(path);

  if (!strongCommercialIntent) {
    addScore(scores, "pricing", -8);
    addScore(scores, "conversion", -8);
  }

  const hardTypes = [
    { type: "policy", min: 100 },
    { type: "contact", min: 90 },
    { type: "about", min: 80 },
  ];

  for (const rule of hardTypes) {
    if ((scores[rule.type] || 0) >= rule.min) return rule.type;
  }

  const winner = bestScoredType(scores, "general");

  if (
    Math.abs((scores.article || 0) - (scores.archive || 0)) <= 8 &&
    Math.max(scores.article || 0, scores.archive || 0) > 40
  ) {
    if (isArticleLikeUrl(normalizedUrl)) return "article";
    if (isArchiveLikeUrl(normalizedUrl)) return "archive";
  }

  if (winner.type === "pricing" || winner.type === "conversion" || winner.type === "service") {
    if (articleBias) return "article";
    if (archiveBias) return "archive";
  }

  return winner.type;
}

function inferSiteTypeFromHomepage(links = []) {
  const counts = {
    ecommerce: 0,
    content: 0,
    service: 0,
  };

  for (const link of links) {
    const href = (link.url || "").toLowerCase();
    const text = (link.anchorText || "").toLowerCase();
    const hay = `${href} ${text}`;

    if (/shop|store|product|products|collection|collections|category|categories|cart/.test(hay)) {
      counts.ecommerce += 2;
    }
    if (/blog|article|articles|news|insights|guides|learn|resources/.test(hay)) {
      counts.content += 2;
    }
    if (/service|services|solution|solutions|pricing|demo|book|consult|contact/.test(hay)) {
      counts.service += 2;
    }
  }

  const sorted = Object.entries(counts).sort((a, b) => b[1] - a[1]);
  if (sorted[0][1] === 0) return "mixed";
  if (sorted[1] && sorted[0][1] === sorted[1][1]) return "mixed";
  return sorted[0][0];
}

function isBadHref(href) {
  if (!href) return true;
  const h = href.trim();
  if (!h || h === "undefined" || h === "null" || h === "#") return true;
  if (/^(mailto:|tel:|javascript:|data:)/i.test(h)) return true;
  return false;
}

function extractNavLinks($, baseUrl) {
  const navSelectors = [
    "header a",
    "nav a",
    "[role='navigation'] a",
    ".menu a",
    ".nav a",
    ".navbar a",
    ".site-header a",
  ];

  const links = [];
  const seen = new Set();

  navSelectors.forEach((selector) => {
    $(selector).each((_, el) => {
      const href = $(el).attr("href");
      const anchorText = cleanText($(el).text() || "");
      if (isBadHref(href)) return;
      const urlObj = safeUrl(href, baseUrl);
      if (!urlObj) return;

      const normalized = normalizeUrl(urlObj.toString());
      if (!normalized || seen.has(normalized)) return;

      seen.add(normalized);
      links.push({ url: normalized, anchorText, source: "nav" });
    });
  });

  return links;
}

function extractInternalLinks($, pageUrl, seedUrl) {
  const results = [];
  const seen = new Set();

  $("a[href]").each((_, el) => {
    const href = $(el).attr("href");
    const anchorText = cleanText($(el).text() || "");

    if (isBadHref(href)) return;

    const fullUrl = safeUrl(href, pageUrl);
    if (!fullUrl) return;

    const normalized = normalizeUrl(fullUrl.toString());
    if (!normalized) return;
    if (!sameHost(normalized, seedUrl)) return;
    if (!isLikelyHtmlUrl(normalized)) return;
    if (seen.has(normalized)) return;

    seen.add(normalized);

    const inNav =
      $(el).closest("nav").length > 0 ||
      $(el).closest("header").length > 0 ||
      $(el).attr("role") === "menuitem";

    results.push({ url: normalized, anchorText, inNav });
  });

  return results;
}

function getUrlFamily(urlString, pageType = "general") {
  const segs = getPathSegments(urlString);
  if (segs.length === 0) return "root";

  const first = segs[0];

  if (pageType === "archive") {
    if (["category", "tag", "author", "page"].includes(first)) {
      return segs.slice(0, 2).join("/") || first;
    }
    return first;
  }

  if (pageType === "article") {
    if (["blog", "news", "guides", "resources", "articles", "posts"].includes(first)) {
      return first;
    }
    return first;
  }

  return first;
}

function createQueueState() {
  return {
    enqueuedTypeCounts: {},
    selectedTypeCounts: {},
    enqueuedFamilyCounts: {},
    selectedFamilyCounts: {},
  };
}

function createSnapshotSummaryState(seedUrl) {
  return {
    seed_url: seedUrl,
    site_type: "mixed",
    pages_crawled: 0,
    errors_count: 0,
    page_type_counts: {},
    score_lists: {
      structural: [],
      visibility: [],
      revenue: [],
      paid_risk: [],
      opportunity: [],
    },
    issues: {
      non_indexable_pages: 0,
      canonical_issues: 0,
      missing_titles: 0,
      missing_meta_descriptions: 0,
      missing_h1s: 0,
      thin_content_pages: 0,
      slow_pages: 0,
      deep_pages: 0,
    },
    top_opportunity_pages: [],
    homepage_text_snippet: "", // body text of first page (for NL API)
    // Location signals accumulated across all crawled pages
    location_signals: {
      uk_phones: 0,
      us_phones: 0,
      au_phones: 0,
      uk_postcodes: 0,
      us_zipcodes: 0,
      au_postcodes: 0,
      gbp_mentions: 0,
      usd_mentions: 0,
      eur_mentions: 0,
      aud_mentions: 0,
      uk_country: 0,
      us_country: 0,
      au_country: 0,
      eu_country: 0,
      ca_country: 0,
      in_country: 0,
    },
  };
}

/**
 * Extract market/location signals from a page's body text.
 * Returns counts of various signals that indicate which country the business is in.
 */
function extractLocationSignals(text) {
  const t = text || "";
  const signals = {
    uk_phones: 0, us_phones: 0, au_phones: 0,
    uk_postcodes: 0, us_zipcodes: 0, au_postcodes: 0,
    gbp_mentions: 0, usd_mentions: 0, eur_mentions: 0, aud_mentions: 0,
    uk_country: 0, us_country: 0, au_country: 0, eu_country: 0,
    ca_country: 0, in_country: 0,
  };

  // UK phone numbers: +44, 07xxx, 01xxx, 02xxx, 08xx, 03xx
  signals.uk_phones = (t.match(/(\+44|0(?:7\d{3}|\d{3,4})\s?\d{3,4}\s?\d{3,4})/g) || []).length;

  // US/Canada phone numbers: +1, (xxx) xxx-xxxx, xxx-xxx-xxxx
  signals.us_phones = (t.match(/(\+1[\s-]?)?\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}/g) || []).length;

  // Australian phone numbers: +61, 04xx, (0x) xxxx xxxx
  signals.au_phones = (t.match(/(\+61|0[45]\d{2}[\s-]?\d{3}[\s-]?\d{3})/g) || []).length;

  // UK postcodes: e.g. SW1A 2AA, EC1A 1BB, W1D 6LT
  signals.uk_postcodes = (t.match(/\b[A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2}\b/g) || []).length;

  // US zip codes: 5 digits, or 5+4
  signals.us_zipcodes = (t.match(/\b\d{5}(?:-\d{4})?\b/g) || []).length;

  // Australian postcodes: 4 digits (less reliable, only count if combined with other AU signals)
  signals.au_postcodes = (t.match(/\bAustralia\b.*?\b\d{4}\b|\b\d{4}\b.*?\bAustralia\b/gi) || []).length;

  // Currency symbols in context of prices
  signals.gbp_mentions = (t.match(/£\s*[\d,]+/g) || []).length;
  signals.usd_mentions = (t.match(/\$\s*[\d,]+/g) || []).length;
  signals.eur_mentions = (t.match(/€\s*[\d,]+/g) || []).length;
  signals.aud_mentions = (t.match(/A\$\s*[\d,]+/gi) || []).length;

  // Country/region mentions (case-insensitive)
  signals.uk_country = (t.match(/\b(United Kingdom|UK|England|Scotland|Wales|Britain|British|London|Manchester|Birmingham|Edinburgh|Glasgow|Bristol|Leeds|Liverpool|Sheffield)\b/gi) || []).length;
  signals.us_country = (t.match(/\b(United States|USA|U\.S\.A|America|American|New York|Los Angeles|Chicago|Houston|Phoenix|Philadelphia|San Francisco|Seattle|Boston|Denver)\b/gi) || []).length;
  signals.au_country = (t.match(/\b(Australia|Australian|Sydney|Melbourne|Brisbane|Perth|Adelaide)\b/gi) || []).length;
  signals.eu_country = (t.match(/\b(Germany|France|Spain|Italy|Netherlands|Belgium|Poland|Sweden|Norway|Denmark|Finland|Austria|Switzerland|Portugal|European Union|EU)\b/gi) || []).length;
  signals.ca_country = (t.match(/\b(Canada|Canadian|Toronto|Vancouver|Montreal|Calgary|Ottawa)\b/gi) || []).length;
  signals.in_country = (t.match(/\b(India|Indian|Mumbai|Delhi|Bangalore|Hyderabad|Chennai|Kolkata|Pune)\b/gi) || []).length;

  return signals;
}

/**
 * Determine market from accumulated location signals across all crawled pages,
 * with TLD-based detection as a fallback/tiebreaker.
 */
function detectMarketFromSignals(locationSignals, seedUrl) {
  const s = locationSignals || {};

  // Score each market (higher = more likely)
  const scores = {
    UK: 0, US: 0, Australia: 0, Europe: 0, Canada: 0, India: 0,
  };

  // Phone numbers (strong signal)
  scores.UK  += (s.uk_phones  || 0) * 4;
  scores.US  += (s.us_phones  || 0) * 3; // US phones overlap with CA
  scores.Australia += (s.au_phones || 0) * 4;

  // Postcodes/zip codes (very strong signal)
  scores.UK  += (s.uk_postcodes || 0) * 6;
  scores.US  += (s.us_zipcodes  || 0) * 2; // 5-digit numbers appear a lot, lower weight
  scores.Australia += (s.au_postcodes || 0) * 5;

  // Currency in prices (strong signal)
  scores.UK      += (s.gbp_mentions || 0) * 5;
  scores.US      += (s.usd_mentions || 0) * 3;
  scores.Europe  += (s.eur_mentions || 0) * 5;
  scores.Australia += (s.aud_mentions || 0) * 5;

  // Country/city name mentions
  scores.UK      += (s.uk_country  || 0) * 3;
  scores.US      += (s.us_country  || 0) * 3;
  scores.Australia += (s.au_country || 0) * 3;
  scores.Europe  += (s.eu_country  || 0) * 3;
  scores.Canada  += (s.ca_country  || 0) * 3;
  scores.India   += (s.in_country  || 0) * 3;

  // Find highest scoring market
  const topMarket = Object.entries(scores).sort((a, b) => b[1] - a[1])[0];
  const topScore = topMarket[1];
  const topName  = topMarket[0];

  // Only trust content signals if we have meaningful evidence (score >= 8)
  if (topScore >= 8) {
    const marketMap = {
      UK:        { market: "UK",        currency: "GBP", symbol: "£",  confidence: 85 },
      US:        { market: "US",        currency: "USD", symbol: "$",  confidence: 80 },
      Australia: { market: "Australia", currency: "AUD", symbol: "A$", confidence: 85 },
      Europe:    { market: "Europe",    currency: "EUR", symbol: "€",  confidence: 82 },
      Canada:    { market: "Canada",    currency: "CAD", symbol: "C$", confidence: 80 },
      India:     { market: "India",     currency: "INR", symbol: "₹",  confidence: 80 },
    };
    console.log(`[market detect] content signals → ${topName} (score=${topScore}) signals=${JSON.stringify(scores)}`);
    return marketMap[topName] || detectMarket(seedUrl);
  }

  // Not enough content signals — fall back to TLD
  console.log(`[market detect] low content signal (top=${topName} score=${topScore}), falling back to TLD`);
  return detectMarket(seedUrl);
}

function incrementCount(map, key) {
  map[key] = (map[key] || 0) + 1;
}

function getCount(map, key) {
  return map[key] || 0;
}

function registerEnqueuedCandidate(queueState, pageType, familyKey) {
  incrementCount(queueState.enqueuedTypeCounts, pageType);
  incrementCount(queueState.enqueuedFamilyCounts, familyKey);
}

function registerSelectedPage(queueState, pageType, familyKey) {
  incrementCount(queueState.selectedTypeCounts, pageType);
  incrementCount(queueState.selectedFamilyCounts, familyKey);
}

function registerSummaryPage(summaryState, pageSummary) {
  summaryState.pages_crawled += 1;
  incrementCount(summaryState.page_type_counts, pageSummary.pageType);

  // Capture homepage body text for Natural Language API (first page only)
  if (summaryState.pages_crawled === 1 && pageSummary.bodyTextSnippet) {
    summaryState.homepage_text_snippet = pageSummary.bodyTextSnippet;
  }

  summaryState.score_lists.structural.push(pageSummary.structuralScore);
  summaryState.score_lists.visibility.push(pageSummary.visibilityScore);
  summaryState.score_lists.revenue.push(pageSummary.revenueScore);
  summaryState.score_lists.paid_risk.push(pageSummary.paidRiskScore);
  summaryState.score_lists.opportunity.push(pageSummary.pageOpportunityScore);

  if (!pageSummary.indexable) summaryState.issues.non_indexable_pages += 1;
  if (!pageSummary.canonicalOk) summaryState.issues.canonical_issues += 1;
  if (!pageSummary.hasTitle) summaryState.issues.missing_titles += 1;
  if (!pageSummary.hasMeta) summaryState.issues.missing_meta_descriptions += 1;
  if (!pageSummary.hasH1) summaryState.issues.missing_h1s += 1;
  if (pageSummary.wordCount < getThinContentThreshold(pageSummary.pageType)) {
    summaryState.issues.thin_content_pages += 1;
  }
  if (pageSummary.loadMs && pageSummary.loadMs > 5000) summaryState.issues.slow_pages += 1;
  if (pageSummary.internalLinkDepth >= 2) summaryState.issues.deep_pages += 1;

  summaryState.top_opportunity_pages.push({
    url: pageSummary.url,
    page_type: pageSummary.pageType,
    opportunity: pageSummary.pageOpportunityScore,
    structural: pageSummary.structuralScore,
    visibility: pageSummary.visibilityScore,
    revenue: pageSummary.revenueScore,
    priority_bucket: pageSummary.priorityBucket,
  });

  summaryState.top_opportunity_pages.sort((a, b) => b.opportunity - a.opportunity);
  summaryState.top_opportunity_pages = summaryState.top_opportunity_pages.slice(0, 5);

  // Accumulate location signals from this page
  if (pageSummary.locationSignals && summaryState.location_signals) {
    const src = pageSummary.locationSignals;
    const dst = summaryState.location_signals;
    for (const key of Object.keys(dst)) {
      dst[key] = (dst[key] || 0) + (src[key] || 0);
    }
  }
}

function getContentMixTargets(maxPages) {
  const usable = Math.max(0, maxPages - 1);
  return {
    minArticles: Math.max(2, Math.min(4, Math.floor(usable * 0.45))),
    maxArchives: Math.max(1, Math.min(2, Math.ceil(usable * 0.25))),
    maxCategories: Math.max(1, Math.min(2, Math.ceil(usable * 0.25))),
    maxSameFamily: 2,
  };
}

function applyQueueV3MixAdjustments({
  score,
  pageType,
  familyKey,
  parentPageType,
  anchorText,
  siteType,
  queueState,
  maxPages,
}) {
  let adjusted = score;
  const selectedTypeCounts = queueState.selectedTypeCounts;
  const selectedFamilyCounts = queueState.selectedFamilyCounts;
  const familySelected = getCount(selectedFamilyCounts, familyKey);
  const familyEnqueued = getCount(queueState.enqueuedFamilyCounts, familyKey);

  if (siteType === "content") {
    const targets = getContentMixTargets(maxPages);
    const selectedArticles = getCount(selectedTypeCounts, "article");
    const selectedArchives = getCount(selectedTypeCounts, "archive");
    const selectedCategories = getCount(selectedTypeCounts, "category");

    if (pageType === "article") {
      adjusted += 18;
      if (selectedArticles < targets.minArticles) adjusted += 24;
      if (parentPageType === "archive" || parentPageType === "category") adjusted += 18;
      if (/read more|continue reading|article|post|story|guide/.test((anchorText || "").toLowerCase())) {
        adjusted += 10;
      }
    }

    if (pageType === "archive") {
      adjusted -= 10;
      if (selectedArchives >= targets.maxArchives) adjusted -= 34;
      if (familySelected >= 1) adjusted -= 12;
      if (familyEnqueued >= 2) adjusted -= 8;
    }

    if (pageType === "category") {
      adjusted -= 4;
      if (selectedCategories >= targets.maxCategories) adjusted -= 24;
      if (familySelected >= 1) adjusted -= 10;
    }

    if (familySelected >= targets.maxSameFamily) adjusted -= 20;
    if (pageType === "general" && parentPageType === "archive") adjusted -= 8;
    if (selectedArticles < targets.minArticles && (pageType === "archive" || pageType === "category")) {
      adjusted -= 12;
    }
  }

  if (siteType === "service" && (pageType === "article" || pageType === "archive")) adjusted -= 8;
  if (siteType === "ecommerce" && (pageType === "article" || pageType === "archive")) adjusted -= 10;

  return adjusted;
}

function buildPriorityScore({
  candidateUrl,
  anchorText,
  depth,
  parentPageType,
  siteType,
  homepageNavSet,
  siblingTypeCounts,
  queueState,
  maxPages,
}) {
  const pageType = classifyPageTypeFromSignals({ url: candidateUrl, anchorText });
  let score = 0;

  const baseByType = {
    homepage: 100,
    conversion: 88,
    pricing: 82,
    service: 80,
    product: 80,
    feature: 70,
    category: 58,
    archive: 46,
    article: 42,
    location: 56,
    case_study: 52,
    proof: 48,
    about: 40,
    general: 36,
    contact: 22,
    policy: 5,
  };

  score += baseByType[pageType] ?? 30;

  if (siteType === "ecommerce") {
    if (pageType === "product") score += 16;
    if (pageType === "category") score += 10;
    if (pageType === "article") score -= 10;
    if (pageType === "archive") score -= 12;
    if (pageType === "service") score -= 6;
  } else if (siteType === "service") {
    if (pageType === "service") score += 18;
    if (pageType === "pricing") score += 12;
    if (pageType === "conversion") score += 12;
    if (pageType === "article") score -= 8;
    if (pageType === "archive") score -= 12;
    if (pageType === "category") score -= 10;
  } else if (siteType === "content") {
    if (pageType === "article") score += 16;
    if (pageType === "archive") score += 4;
    if (pageType === "case_study") score += 4;
    if (pageType === "category") score -= 6;
    if (pageType === "product") score -= 8;
    if (pageType === "pricing") score -= 8;
    if (pageType === "conversion") score -= 8;
  }

  if (homepageNavSet.has(candidateUrl)) score += 18;

  if (parentPageType === "homepage") score += 8;
  if (parentPageType === "category" && pageType === "product") score += 10;
  if (parentPageType === "homepage" && pageType === "service") score += 8;
  if (parentPageType === "homepage" && pageType === "pricing") score += 8;
  if (parentPageType === "archive" && pageType === "article") score += 16;
  if (parentPageType === "category" && pageType === "article") score += 12;
  if (parentPageType === "article" && pageType === "article") score -= 6;

  const anchor = (anchorText || "").toLowerCase();
  if (/pricing|plans|book demo|demo|trial|get started|contact sales/.test(anchor)) score += 10;
  if (/services|solutions|products|shop|store|collections/.test(anchor)) score += 6;
  if (/read more|continue reading|learn more|article|post|story|guide/.test(anchor) && pageType === "article") {
    score += 8;
  }
  if (/privacy|terms|cookie|refund|shipping/.test(anchor)) score -= 18;

  score += scoreSlugHint(candidateUrl);
  score -= depth * 10;

  const segs = getPathSegments(candidateUrl);
  if (segs.length <= 1) score += 6;
  else if (segs.length >= 4) score -= 6;

  const currentCount = siblingTypeCounts[pageType] || 0;
  if (pageType === "category" && currentCount >= 2) score -= 18;
  if (pageType === "archive" && currentCount >= 2) score -= 20;
  if (pageType === "article" && currentCount >= 4) score -= 8;
  if (pageType === "policy" && currentCount >= 1) score -= 24;
  if (pageType === "contact" && currentCount >= 1) score -= 12;
  if (pageType === "product" && currentCount >= 3) score -= 10;

  const familyKey = getUrlFamily(candidateUrl, pageType);

  score = applyQueueV3MixAdjustments({
    score,
    pageType,
    familyKey,
    parentPageType,
    anchorText,
    siteType,
    queueState,
    maxPages,
  });

  if (pageType === "policy") score = Math.min(score, 18);
  if (pageType === "contact") score = Math.min(score, 38);

  return { score, pageType, familyKey };
}

function getPageIntentWeight(pageType) {
  const map = {
    homepage: 0.88,
    conversion: 1.0,
    pricing: 0.96,
    service: 0.9,
    product: 0.9,
    category: 0.72,
    feature: 0.68,
    case_study: 0.62,
    proof: 0.58,
    article: 0.45,
    archive: 0.22,
    about: 0.3,
    location: 0.74,
    general: 0.38,
    contact: 0.16,
    policy: 0.04,
  };
  return map[pageType] ?? 0.35;
}

function getVisibilityPotential(pageType) {
  const map = {
    homepage: 0.92,
    conversion: 0.82,
    pricing: 0.86,
    service: 0.88,
    product: 0.86,
    category: 0.78,
    feature: 0.68,
    case_study: 0.6,
    proof: 0.52,
    article: 0.84,
    archive: 0.5,
    about: 0.42,
    location: 0.7,
    general: 0.48,
    contact: 0.18,
    policy: 0.05,
  };
  return map[pageType] ?? 0.45;
}

function getThinContentThreshold(pageType) {
  const thresholds = {
    homepage: 250,
    service: 500,
    pricing: 350,
    conversion: 220,
    product: 250,
    category: 180,
    article: 700,
    archive: 120,
    feature: 300,
    case_study: 450,
    proof: 180,
    about: 250,
    location: 250,
    general: 250,
    contact: 80,
    policy: 80,
  };

  return thresholds[pageType] ?? 250;
}

function computeContentDepthScore(wordCount, pageType) {
  const threshold = getThinContentThreshold(pageType);

  if (wordCount <= 0) return 0;
  if (wordCount >= threshold * 1.75) return 100;
  if (wordCount >= threshold * 1.2) return 85;
  if (wordCount >= threshold) return 70;
  if (wordCount >= threshold * 0.7) return 45;
  if (wordCount >= threshold * 0.4) return 25;
  return 10;
}

function computeStructuralScore({
  indexable,
  canonicalOk,
  hasTitle,
  hasMeta,
  hasH1,
  wordCount,
  statusCode,
  pageType,
  loadMs,
  schemaTypes,
}) {
  let score = 0;

  if (statusCode >= 200 && statusCode < 300) score += 16;
  if (indexable) score += 18;
  if (hasTitle) score += 14;
  if (hasMeta) score += 10;
  if (hasH1) score += 12;
  if (canonicalOk) score += 10;

  const contentDepthScore = computeContentDepthScore(wordCount, pageType);
  score += Math.round(contentDepthScore * 0.14);

  if ((schemaTypes || []).length > 0) score += 5;
  if (loadMs && loadMs < 1200) score += 6;
  else if (loadMs && loadMs < 2500) score += 4;
  else if (loadMs && loadMs > 5000) score -= 4;

  return clamp(Math.round(score), 0, 100);
}

function computeVisibilityScore({
  indexable,
  structuralScore,
  pageType,
  statusCode,
  internalLinkDepth,
  noindex,
  loadMs,
}) {
  let score = 0;

  const visibilityPotential = getVisibilityPotential(pageType);
  score += visibilityPotential * 40;
  score += structuralScore * 0.38;

  if (statusCode >= 200 && statusCode < 300) score += 10;
  if (indexable) score += 12;
  if (noindex) score -= 22;

  if (internalLinkDepth === 0) score += 10;
  else if (internalLinkDepth === 1) score += 6;
  else if (internalLinkDepth >= 3) score -= 6;

  if (loadMs && loadMs > 5000) score -= 5;

  return clamp(Math.round(score), 0, 100);
}

function computeRevenueScore(pageType) {
  return clamp(Math.round(getPageIntentWeight(pageType) * 100), 0, 100);
}

function computePaidRiskScore({
  pageType,
  indexable,
  structuralScore,
  visibilityScore,
  loadMs,
}) {
  let score = 0;

  const intentWeight = getPageIntentWeight(pageType);
  score += intentWeight * 42;

  if (!indexable) score += 24;
  if (structuralScore < 65) score += 18;
  if (structuralScore < 45) score += 10;
  if (visibilityScore < 55) score += 10;
  if (loadMs && loadMs > 5000) score += 6;
  if (["policy", "contact", "about"].includes(pageType)) score -= 18;

  return clamp(Math.round(score), 0, 100);
}

function computePageOpportunityScore({
  pageType,
  structuralScore,
  visibilityScore,
  revenueScore,
  indexable,
  statusCode,
  wordCount,
  internalLinkDepth,
  canonicalOk,
  hasTitle,
  hasMeta,
  hasH1,
}) {
  const weakness = 100 - structuralScore;
  const visibilityGap = 100 - visibilityScore;
  const contentDepthScore = computeContentDepthScore(wordCount, pageType);
  const contentGap = 100 - contentDepthScore;
  const intentWeight = getPageIntentWeight(pageType);
  const visibilityPotential = getVisibilityPotential(pageType);

  let score = 0;

  score += weakness * 0.28;
  score += visibilityGap * 0.18;
  score += revenueScore * 0.28;
  score += visibilityPotential * 14;
  score += contentGap * 0.12;

  if (!indexable) score += 14;
  if (statusCode >= 400) score += 14;
  if (!canonicalOk) score += 5;
  if (!hasTitle) score += 6;
  if (!hasMeta) score += 5;
  if (!hasH1) score += 5;
  if (internalLinkDepth >= 2) score += 5;
  if (internalLinkDepth >= 3) score += 4;

  if (pageType === "article" && weakness >= 35) score += 6;
  if (pageType === "homepage" && weakness >= 20) score += 8;
  if (["pricing", "conversion", "service", "product"].includes(pageType) && weakness >= 20) {
    score += 10;
  }

  if (["archive", "policy", "contact"].includes(pageType)) score -= 10;
  if (pageType === "about") score -= 4;
  if (intentWeight <= 0.1) score = Math.min(score, 24);

  return clamp(Math.round(score), 0, 100);
}

function computePriorityBucket(opportunityScore, revenueScore, pageType) {
  if (["policy", "contact"].includes(pageType) && opportunityScore < 30) return "Tier 4";
  if (opportunityScore >= 78) return "Tier 1";
  if (opportunityScore >= 58) return "Tier 2";
  if (opportunityScore >= 35) return "Tier 3";
  return "Tier 4";
}

function getActionCap(pageType, pageOpportunityScore) {
  if (["homepage", "pricing", "conversion", "service", "product"].includes(pageType)) {
    return pageOpportunityScore >= 70 ? 5 : 4;
  }
  if (["article", "category", "feature", "case_study"].includes(pageType)) return 4;
  if (["archive", "about", "location"].includes(pageType)) return 3;
  if (["contact", "policy"].includes(pageType)) return 2;
  return 3;
}

function getActionPriorityFromScore(score) {
  if (score >= 90) return "critical";
  if (score >= 72) return "high";
  if (score >= 52) return "medium";
  return "low";
}

function getActionSeverityFromScore(score) {
  if (score >= 85) return "high";
  if (score >= 55) return "medium";
  return "low";
}

function dedupeAndLimitActions(actions, pageType, pageOpportunityScore) {
  const seen = new Set();
  const deduped = [];

  for (const action of actions.sort((a, b) => b._sortScore - a._sortScore)) {
    const key = `${action.action_type}::${action.title}`;
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(action);
  }

  const cap = getActionCap(pageType, pageOpportunityScore);
  return deduped.slice(0, cap).map(({ _sortScore, ...rest }) => rest);
}

function buildActions({
  pageType,
  statusCode,
  indexable,
  canonicalOk,
  hasTitle,
  hasMeta,
  hasH1,
  wordCount,
  title,
  metaDescription,
  h1Count,
  pageOpportunityScore,
  structuralScore,
  visibilityScore,
  revenueScore,
  internalLinkDepth,
  loadMs,
}) {
  const actions = [];
  const thinThreshold = getThinContentThreshold(pageType);
  const isCommercial = ["homepage", "pricing", "conversion", "service", "product"].includes(pageType);

  const pushAction = ({
    actionType,
    titleText,
    summary,
    whyItMatters,
    technicalReason,
    expectedImpactRange,
    steps,
    score,
  }) => {
    actions.push({
      action_type: actionType,
      summary,
      priority: getActionPriorityFromScore(score),
      status: "pending",
      title: titleText,
      why_it_matters: whyItMatters,
      technical_reason: technicalReason,
      expected_impact_range: expectedImpactRange,
      steps,
      severity: getActionSeverityFromScore(score),
      _sortScore: score,
    });
  };

  if (statusCode >= 400) {
    pushAction({
      actionType: "fix_status_code",
      titleText: `Fix HTTP ${statusCode} page issue`,
      summary: `This page returns HTTP ${statusCode} and may be blocking SEO visibility.`,
      whyItMatters: "Broken or erroring pages waste crawl equity and prevent search visibility.",
      technicalReason: `The crawler received HTTP ${statusCode} for this page.`,
      expectedImpactRange: isCommercial ? "High" : "Medium-High",
      steps: [
        "Confirm whether this URL should remain live.",
        "Restore the page if it should exist.",
        "Redirect it to the closest relevant page if it should not exist.",
        "Update internal links pointing to this URL."
      ],
      score: isCommercial ? 98 : 92,
    });
  }

  if (!indexable && statusCode >= 200 && statusCode < 300) {
    pushAction({
      actionType: "review_indexability",
      titleText: "Review indexability settings",
      summary: "This page is live but may not be indexable.",
      whyItMatters: "A live page that cannot be indexed will struggle to earn organic traffic.",
      technicalReason: "The page appears live, but crawl signals suggest it may not be intended for indexation.",
      expectedImpactRange: isCommercial ? "High" : "Medium-High",
      steps: [
        "Check robots meta directives on the page.",
        "Confirm whether noindex is intentional.",
        "Remove noindex from pages that should rank."
      ],
      score: isCommercial ? 95 : 86,
    });
  }

  if (!hasTitle || !title || title.length < 20) {
    pushAction({
      actionType: "improve_title",
      titleText: "Improve page title",
      summary: "The page title is missing or too weak.",
      whyItMatters: "Titles are a major relevance and click-through signal in search results.",
      technicalReason: "The page title is missing or too short to communicate page intent clearly.",
      expectedImpactRange: isCommercial ? "Medium-High" : "Medium",
      steps: [
        "Write a unique title for this page.",
        "Place the main topic near the beginning.",
        "Match the title to the actual page intent."
      ],
      score: isCommercial ? 84 : 72,
    });
  }

  if (!hasMeta || !metaDescription || metaDescription.length < 110) {
    pushAction({
      actionType: "improve_meta_description",
      titleText: "Improve meta description",
      summary: "The meta description is missing or too short.",
      whyItMatters: "A clearer meta description can improve SERP click-through rate.",
      technicalReason: "The page lacks a sufficiently descriptive meta description.",
      expectedImpactRange: isCommercial ? "Medium" : "Low-Medium",
      steps: [
        "Write a concise description of 140 to 160 characters.",
        "Reflect the user intent of the page.",
        "Include a clear value proposition or reason to click."
      ],
      score: isCommercial ? 70 : 56,
    });
  }

  if (!hasH1) {
    pushAction({
      actionType: "add_h1",
      titleText: "Add a clear H1",
      summary: "This page has no H1 heading.",
      whyItMatters: "A primary H1 helps users and search engines understand page focus.",
      technicalReason: "No H1 element was found on the page.",
      expectedImpactRange: "Medium",
      steps: [
        "Add one primary H1 to the page.",
        "Align it with the page's main intent.",
        "Avoid creating multiple competing H1s."
      ],
      score: isCommercial ? 82 : 74,
    });
  } else if (h1Count > 1) {
    pushAction({
      actionType: "reduce_multiple_h1s",
      titleText: "Reduce multiple H1 tags",
      summary: "This page has multiple H1 tags.",
      whyItMatters: "Multiple H1s can weaken heading hierarchy and topical clarity.",
      technicalReason: `${h1Count} H1 tags were found.`,
      expectedImpactRange: "Low-Medium",
      steps: [
        "Keep one primary H1 on the page.",
        "Convert secondary headings into H2 or H3."
      ],
      score: 56,
    });
  }

  if (!canonicalOk) {
    pushAction({
      actionType: "review_canonical",
      titleText: "Review canonical tag",
      summary: "Canonical setup may be missing or not aligned to this page.",
      whyItMatters: "Incorrect canonicals can split ranking signals or suppress the intended URL.",
      technicalReason: "The page canonical is missing or does not match the crawled final URL.",
      expectedImpactRange: isCommercial ? "Medium" : "Low-Medium",
      steps: [
        "Check whether the page should self-canonicalize.",
        "Fix the canonical if it points to the wrong URL.",
        "Consolidate duplicate variants to the preferred page."
      ],
      score: isCommercial ? 64 : 48,
    });
  }

  if (wordCount < thinThreshold && ["homepage", "service", "pricing", "product", "category", "article", "general", "feature", "case_study"].includes(pageType)) {
    pushAction({
      actionType: "expand_content",
      titleText: "Expand thin content",
      summary: "The page content looks thin for its intent.",
      whyItMatters: "Thin pages often struggle to rank or convert because they do not fully satisfy user intent.",
      technicalReason: `The page appears to contain about ${wordCount} words, which is below the expected depth for this page type.`,
      expectedImpactRange: isCommercial ? "Medium-High" : "Medium",
      steps: [
        "Add deeper and more useful information related to the page topic.",
        "Answer common user questions directly.",
        "Use stronger sections and supporting headings to improve topical depth."
      ],
      score: isCommercial ? 78 : pageType === "article" ? 74 : 62,
    });
  }

  if (pageType === "homepage" && pageOpportunityScore >= 55) {
    pushAction({
      actionType: "strengthen_homepage_seo_hub",
      titleText: "Strengthen homepage as SEO hub",
      summary: "The homepage should do a stronger job routing relevance and authority across the site.",
      whyItMatters: "The homepage is usually the strongest authority hub and a major entry point for branded demand.",
      technicalReason: "The page has strategic importance but still shows notable structural or visibility gaps.",
      expectedImpactRange: "High",
      steps: [
        "Clarify the core positioning of the business in the title and headline.",
        "Improve links from the homepage to key revenue or strategic pages.",
        "Strengthen homepage copy so it communicates intent more clearly."
      ],
      score: 80,
    });
  }

  if (["pricing", "service", "product", "conversion"].includes(pageType) && pageOpportunityScore >= 60) {
    pushAction({
      actionType: "prioritize_commercial_page",
      titleText: "Prioritize this commercial page",
      summary: "This is a commercially important page with meaningful SEO upside.",
      whyItMatters: "Improvements on high-intent pages can create outsized business value.",
      technicalReason: "The page combines strong revenue intent with optimization gaps.",
      expectedImpactRange: "High",
      steps: [
        "Prioritize technical and on-page improvements here first.",
        "Improve internal linking into this page from stronger site sections.",
        "Make the offer and differentiation clearer."
      ],
      score: pageOpportunityScore >= 80 ? 94 : 82,
    });
  }

  if (pageType === "article" && wordCount < thinThreshold) {
    pushAction({
      actionType: "improve_article_depth",
      titleText: "Improve article depth and completeness",
      summary: "This article likely needs more depth to compete for informational queries.",
      whyItMatters: "Informational pages often need stronger coverage to rank for broader and more competitive topics.",
      technicalReason: "The article appears thin relative to the expected threshold for informational content.",
      expectedImpactRange: "Medium",
      steps: [
        "Expand the article to cover subtopics, definitions, FAQs, or examples.",
        "Add stronger section structure and semantic breadth.",
        "Make sure the article fully satisfies search intent."
      ],
      score: 72,
    });
  }

  if (pageType === "article" && internalLinkDepth >= 2) {
    pushAction({
      actionType: "improve_internal_linking_to_article",
      titleText: "Improve internal linking to this article",
      summary: "This article sits relatively deep in the site structure.",
      whyItMatters: "Important content can struggle when it is too far from stronger pages or hubs.",
      technicalReason: `The page was discovered at internal depth ${internalLinkDepth}.`,
      expectedImpactRange: "Low-Medium",
      steps: [
        "Link to this article from relevant archive or category pages.",
        "Add contextual links from related articles.",
        "Promote important evergreen content from stronger site sections."
      ],
      score: 58,
    });
  }

  if (pageType === "archive") {
    pushAction({
      actionType: "strengthen_archive_hub_role",
      titleText: "Strengthen archive page as a content hub",
      summary: "This archive page should better distribute authority to high-value detail pages.",
      whyItMatters: "Archive pages work best when they help users and search engines reach the strongest articles efficiently.",
      technicalReason: "The page type is archive-like, which usually performs best as a structured hub rather than a weak list.",
      expectedImpactRange: "Low-Medium",
      steps: [
        "Improve descriptive intro copy on the archive page.",
        "Highlight top or evergreen detail pages more clearly.",
        "Make sure the archive supports crawl flow into important articles."
      ],
      score: 54,
    });
  }

  if (pageType === "category") {
    pushAction({
      actionType: "clarify_category_intent",
      titleText: "Clarify category page intent",
      summary: "This category page should better explain what users can find here.",
      whyItMatters: "Category pages often perform better when they combine clear topic framing with strong onward links.",
      technicalReason: "The page type suggests a category page, which may need stronger explanatory copy.",
      expectedImpactRange: "Low-Medium",
      steps: [
        "Add a short descriptive intro that explains the category.",
        "Improve internal links to the best detail pages in the category.",
        "Avoid making the page a thin list with little context."
      ],
      score: 52,
    });
  }

  if (visibilityScore < 55 && structuralScore >= 70 && internalLinkDepth >= 2) {
    pushAction({
      actionType: "improve_internal_prominence",
      titleText: "Improve internal prominence",
      summary: "This page looks reasonably well built, but may be too weakly connected internally.",
      whyItMatters: "Pages can remain underexposed when they are structurally decent but buried in the site.",
      technicalReason: "Structural quality is acceptable, but visibility readiness is still being held back and the page sits relatively deep.",
      expectedImpactRange: "Medium",
      steps: [
        "Increase internal links from stronger site sections.",
        "Link to this page from relevant parent or hub pages.",
        "Make sure anchor text reflects the page topic clearly."
      ],
      score: 66,
    });
  }

  if (loadMs && loadMs > 5000) {
    pushAction({
      actionType: "improve_page_speed",
      titleText: "Improve page load speed",
      summary: "This page loaded slowly during the crawl.",
      whyItMatters: "Slow pages can harm user experience and may hold back search performance.",
      technicalReason: `The crawler recorded a load time of about ${loadMs} ms.`,
      expectedImpactRange: "Low-Medium",
      steps: [
        "Review large assets and unnecessary scripts on the page.",
        "Improve caching, compression, and delivery where possible.",
        "Check whether third-party scripts are causing delay."
      ],
      score: isCommercial ? 62 : 50,
    });
  }

  if (pageOpportunityScore >= 80 && ["homepage", "pricing", "conversion", "service", "product"].includes(pageType)) {
    pushAction({
      actionType: "make_this_an_early_seo_win",
      titleText: "Make this an early SEO win page",
      summary: "This page should be moved near the top of the roadmap.",
      whyItMatters: "High-value pages with clear fixable gaps are often the fastest way to create visible SEO momentum.",
      technicalReason: "The page combines high business importance with meaningful SEO opportunity.",
      expectedImpactRange: "High",
      steps: [
        "Resolve the highest-priority technical issues first.",
        "Strengthen page relevance and messaging.",
        "Support it with stronger internal links from strategic pages."
      ],
      score: 90,
    });
  }

  return dedupeAndLimitActions(actions, pageType, pageOpportunityScore);
}

function buildSnapshotSummary(summaryState) {
  const pagesCrawled = summaryState.pages_crawled || 0;
  const pageTypeCounts = summaryState.page_type_counts || {};
  const issues = summaryState.issues || {};

  const avgStructural = avg(summaryState.score_lists.structural);
  const avgVisibility = avg(summaryState.score_lists.visibility);
  const avgRevenue = avg(summaryState.score_lists.revenue);
  const avgPaidRisk = avg(summaryState.score_lists.paid_risk);
  const avgOpportunity = avg(summaryState.score_lists.opportunity);

  const topIssues = [
    { key: "missing_meta_descriptions", label: "Missing meta descriptions", count: issues.missing_meta_descriptions || 0 },
    { key: "thin_content_pages", label: "Thin content pages", count: issues.thin_content_pages || 0 },
    { key: "missing_h1s", label: "Missing H1s", count: issues.missing_h1s || 0 },
    { key: "canonical_issues", label: "Canonical issues", count: issues.canonical_issues || 0 },
    { key: "non_indexable_pages", label: "Non-indexable pages", count: issues.non_indexable_pages || 0 },
    { key: "slow_pages", label: "Slow pages", count: issues.slow_pages || 0 },
    { key: "deep_pages", label: "Deep pages", count: issues.deep_pages || 0 },
  ]
    .sort((a, b) => b.count - a.count)
    .slice(0, 4);

  const focusAreas = [];
  if (avgOpportunity >= 60) focusAreas.push("High overall SEO opportunity across crawled pages");
  if ((issues.thin_content_pages || 0) >= 2) focusAreas.push("Content depth is a recurring issue");
  if ((issues.missing_meta_descriptions || 0) >= 2) focusAreas.push("Snippet optimization needs attention");
  if ((issues.non_indexable_pages || 0) >= 1) focusAreas.push("Indexability should be reviewed");
  if ((issues.deep_pages || 0) >= 2) focusAreas.push("Important pages may be too deep in the site structure");

  return {
    version: 1,
    generated_at: nowIso(),
    site_type: summaryState.site_type || "mixed",
    pages_crawled: pagesCrawled,
    errors: summaryState.errors_count || 0,
    page_type_mix: pageTypeCounts,
    avg_scores: {
      structural: avgStructural,
      visibility: avgVisibility,
      revenue: avgRevenue,
      paid_risk: avgPaidRisk,
      opportunity: avgOpportunity,
    },
    issues,
    top_issues: topIssues,
    top_opportunity_pages: (summaryState.top_opportunity_pages || []).slice(0, 5),
    focus_areas: focusAreas.slice(0, 4),
  };
}

async function updateSnapshotSummary(snapshotId, summaryJson) {
  const { error } = await supabase
    .from("scc_snapshots")
    .update({
      notes: JSON.stringify(summaryJson, null, 2),
    })
    .eq("id", snapshotId);

  if (error) {
    console.error(`[snapshot summary update] snapshot=${snapshotId}`, error.message);
  }
}

// ─── HTTP fetch ──────────────────────────────────────────────────────────────

// One cookie jar per crawl job — persists cookies (e.g. cf_clearance) across pages
const jobCookieJars = new Map();

function getCookieJar(jobId) {
  if (!jobCookieJars.has(jobId)) {
    jobCookieJars.set(jobId, new CookieJar());
  }
  return jobCookieJars.get(jobId);
}

function deleteCookieJar(jobId) {
  jobCookieJars.delete(jobId);
}

function buildBrowserHeaders(url, referer) {
  const origin = (() => { try { const u = new URL(url); return `${u.protocol}//${u.host}`; } catch { return ""; } })();
  return {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control": "max-age=0",
    "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": referer ? "same-origin" : "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "Connection": "keep-alive",
    ...(referer ? { "Referer": referer } : {}),
    ...(origin ? { "Origin": origin } : {}),
  };
}

async function fetchHtml(url, { jobId = null, referer = null } = {}) {
  const startMs = Date.now();

  const jar = jobId ? getCookieJar(jobId) : null;

  // Read cookies for this URL from the jar
  const cookieHeader = jar ? await jar.getCookieString(url).catch(() => "") : "";

  const headers = {
    ...buildBrowserHeaders(url, referer),
    ...(cookieHeader ? { "Cookie": cookieHeader } : {}),
  };

  const response = await axios.get(url, {
    timeout: REQUEST_TIMEOUT_MS,
    maxRedirects: MAX_REDIRECTS,
    headers,
    responseType: "text",
    validateStatus: () => true,
    decompress: true,
  });

  const loadMs = Date.now() - startMs;

  // Persist Set-Cookie headers into the jar
  if (jar) {
    const setCookies = response.headers["set-cookie"] || [];
    const list = Array.isArray(setCookies) ? setCookies : [setCookies];
    for (const raw of list) {
      await jar.setCookie(raw, url).catch(() => {});
    }
  }

  const finalUrl =
    response.request?.res?.responseUrl ||
    response.request?.responseURL ||
    url;

  return {
    finalUrl,
    html: typeof response.data === "string" ? response.data : "",
    status: response.status,
    contentType: response.headers["content-type"] || "",
    loadMs,
  };
}

// ─── HTML extraction ─────────────────────────────────────────────────────────

function extractSeoData(html, url, statusCode, contentType, loadMs, depth, seedUrl) {
  const $ = cheerio.load(html || "");

  const title = cleanText($("title").first().text() || "");
  const metaDescription = cleanText(
    $('meta[name="description"]').attr("content") || ""
  );
  const canonicalUrl = $('link[rel="canonical"]').attr("href") || null;

  const h1Elements = $("h1");
  const h1Count = h1Elements.length;
  const h1Text = cleanText(h1Elements.first().text() || "");

  const robotsMeta = $('meta[name="robots"]').attr("content") || null;
  const noindex = robotsMeta ? /noindex/i.test(robotsMeta) : false;
  const indexable = statusCode >= 200 && statusCode < 300 && !noindex;

  const schemaTypes = detectSchemaTypes($);

  // Remove script/style/noscript before extracting text so NL API gets clean prose
  const $body = $("body").clone();
  $body.find("script, style, noscript, [aria-hidden='true']").remove();
  const bodyText = cleanText($body.text() || "");
  const wordCount = countWords(bodyText);
  const locationSignals = extractLocationSignals(bodyText);
  const bodyTextSnippet = bodyText.slice(0, 5000);

  const pageType = classifyPageTypeFromSignals({
    url,
    title,
    h1Text,
    bodyText: bodyText.slice(0, 2000),
    schemaTypes,
  });

  // Collect unique internal links
  const internalLinks = [];
  const seenLinks = new Set();

  $("a[href]").each((_, el) => {
    const href = $(el).attr("href") || "";
    if (isBadHref(href)) return;
    const resolved = safeUrl(href, url);
    if (!resolved) return;
    if (resolved.protocol !== "http:" && resolved.protocol !== "https:") return;
    if (!sameHost(resolved.toString(), seedUrl)) return;
    const normalized = normalizeUrl(resolved.toString());
    if (!normalized || seenLinks.has(normalized)) return;
    seenLinks.add(normalized);
    internalLinks.push({
      url: normalized,
      anchorText: cleanText($(el).text() || ""),
    });
  });

  return {
    title,
    metaDescription,
    canonicalUrl,
    h1Count,
    h1Text,
    wordCount,
    robotsMeta,
    noindex,
    indexable,
    schemaTypes,
    pageType,
    internalLinks,
    statusCode,
    contentType,
    loadMs,
    locationSignals,
    bodyTextSnippet,
  };
}

// ─── Canonical validation ────────────────────────────────────────────────────

function evaluateCanonicalOk(finalUrl, canonicalUrl) {
  // No canonical tag is not inherently wrong
  if (!canonicalUrl) return true;

  const normalFinal = normalizeUrl(finalUrl) || finalUrl;
  const normalCanonical = normalizeUrl(canonicalUrl) || canonicalUrl;
  return normalFinal === normalCanonical;
}

// ─── Database helpers ────────────────────────────────────────────────────────

async function getOrCreatePage({ siteId, url, pageType }) {
  // Try existing first
  const { data: existing } = await supabase
    .from("scc_pages")
    .select("id")
    .eq("site_id", siteId)
    .eq("url", url)
    .maybeSingle();

  if (existing?.id) {
    await supabase
      .from("scc_pages")
      .update({ page_type: pageType, last_seen_at: nowIso() })
      .eq("id", existing.id);
    return existing.id;
  }

  const { data, error } = await supabase
    .from("scc_pages")
    .insert({
      site_id: siteId,
      url,
      page_type: pageType,
      first_seen_at: nowIso(),
      last_seen_at: nowIso(),
    })
    .select("id")
    .single();

  if (error) throw new Error(`getOrCreatePage failed: ${error.message}`);
  return data.id;
}

async function upsertPageSnapshotCrawl({ snapshotId, pageId, crawlRow }) {
  const { error } = await supabase
    .from("scc_page_snapshot_crawl")
    .upsert(
      { snapshot_id: snapshotId, page_id: pageId, ...crawlRow },
      { onConflict: "snapshot_id,page_id" }
    );

  if (error) {
    console.error(`[upsertPageSnapshotCrawl] page=${pageId}`, error.message);
  }
}

async function upsertPageSnapshotMetrics({ snapshotId, pageId, metricsRow }) {
  const { error } = await supabase
    .from("scc_page_snapshot_metrics")
    .upsert(
      { snapshot_id: snapshotId, page_id: pageId, ...metricsRow },
      { onConflict: "snapshot_id,page_id" }
    );

  if (error) {
    console.error(`[upsertPageSnapshotMetrics] page=${pageId}`, error.message);
  }
}

async function replaceActions({ snapshotId, pageId, actions }) {
  // Clear previous actions for this page in this snapshot
  const { error: deleteError } = await supabase
    .from("scc_actions")
    .delete()
    .eq("snapshot_id", snapshotId)
    .eq("page_id", pageId);

  if (deleteError) {
    console.error(`[replaceActions delete] page=${pageId}`, deleteError.message);
    return;
  }

  if (!actions.length) return;

  const rows = actions.map((action) => ({
    snapshot_id:           snapshotId,
    page_id:               pageId,
    action_type:           action.action_type,
    title:                 action.title,
    summary:               action.summary,
    why_it_matters:        action.why_it_matters,
    technical_reason:      action.technical_reason,
    expected_impact_range: action.expected_impact_range,
    steps:                 action.steps,
    severity:              action.severity,
    priority:              action.priority,
    status:                "pending",
  }));

  const { error: insertError } = await supabase
    .from("scc_actions")
    .insert(rows);

  if (insertError) {
    console.error(`[replaceActions insert] page=${pageId}`, insertError.message);
  }
}

async function generateSiteWideActions(snapshotId, summaryState) {
  const { pages_crawled, page_type_counts, issues, score_lists } = summaryState;
  if (!pages_crawled || pages_crawled === 0) return;

  const actions = [];
  const avgStructural = avg(score_lists.structural);
  const pct = (n) => Math.round((n / pages_crawled) * 100);

  if (issues.missing_meta_descriptions > 0) {
    const p = pct(issues.missing_meta_descriptions);
    actions.push({
      action_type: "site_missing_meta_descriptions",
      title: `${issues.missing_meta_descriptions} of ${pages_crawled} pages missing meta descriptions`,
      severity: p >= 50 ? "high" : "medium",
      why_it_matters: "Meta descriptions influence click-through rates from search results. Missing them means Google auto-generates snippets, often poorly.",
      technical_reason: `${p}% of crawled pages have no meta description tag.`,
      expected_impact_range: p >= 50 ? "High" : "Medium-High",
      steps: [
        "Audit all pages missing meta descriptions.",
        "Write unique, keyword-rich descriptions of 120–160 characters for each.",
        "Prioritise your highest-traffic and highest-opportunity pages first.",
      ],
    });
  }

  if (issues.missing_h1s > 0) {
    const p = pct(issues.missing_h1s);
    actions.push({
      action_type: "site_missing_h1s",
      title: `${issues.missing_h1s} of ${pages_crawled} pages missing H1 tags`,
      severity: p >= 50 ? "high" : "medium",
      why_it_matters: "H1 tags are the strongest on-page relevance signal. Missing H1s weaken topical clarity for search engines.",
      technical_reason: `${p}% of crawled pages have no H1 element.`,
      expected_impact_range: "Medium",
      steps: [
        "Add a single, descriptive H1 to every page.",
        "Make the H1 reflect the primary keyword or topic of the page.",
        "Ensure H1 is distinct from the title tag but complementary.",
      ],
    });
  }

  if (issues.non_indexable_pages > 0 && issues.non_indexable_pages < pages_crawled) {
    actions.push({
      action_type: "site_non_indexable_pages",
      title: `${issues.non_indexable_pages} page${issues.non_indexable_pages > 1 ? "s" : ""} blocked from indexing`,
      severity: "high",
      why_it_matters: "Non-indexable pages are invisible to search engines and generate zero organic traffic.",
      technical_reason: "These pages have noindex directives or canonical tags pointing elsewhere.",
      expected_impact_range: "High",
      steps: [
        "Review each non-indexable page and confirm it should be blocked.",
        "For pages that should rank, remove noindex tags and fix canonical issues.",
        "Submit corrected pages to Google Search Console for re-indexing.",
      ],
    });
  }

  if (issues.thin_content_pages >= 2) {
    const p = pct(issues.thin_content_pages);
    actions.push({
      action_type: "site_thin_content",
      title: `${issues.thin_content_pages} pages have thin content`,
      severity: p >= 50 ? "high" : "medium",
      why_it_matters: "Thin content dilutes topical authority and suppresses rankings across your whole site, not just the thin pages.",
      technical_reason: `${p}% of crawled pages fall below the word-count threshold for their page type.`,
      expected_impact_range: "Medium-High",
      steps: [
        "Identify thin pages and decide: expand, consolidate, or noindex.",
        "Prioritise expanding content on commercial and service pages first.",
        "Consider merging thin related pages into comprehensive hub pages.",
      ],
    });
  }

  if (avgStructural < 40 && pages_crawled >= 2) {
    actions.push({
      action_type: "site_low_structural",
      title: `Site-wide structural SEO is weak (avg score: ${avgStructural}/100)`,
      severity: "high",
      why_it_matters: "Structural SEO is the foundation. A low average score indicates systemic technical debt that suppresses all pages simultaneously.",
      technical_reason: `Average structural score across ${pages_crawled} pages is ${avgStructural}/100.`,
      expected_impact_range: "High",
      steps: [
        "Work through the per-page recommendations starting with highest-opportunity pages.",
        "Fix missing titles and meta descriptions across all pages.",
        "Ensure every page has a clear H1 and proper heading hierarchy.",
        "Improve internal linking so all key pages are reachable within 2 clicks from the homepage.",
      ],
    });
  }

  const hasService = (page_type_counts.service || 0) > 0;
  const hasProduct = (page_type_counts.product || 0) > 0;
  const hasPricing = (page_type_counts.pricing || 0) > 0;
  const hasArticle = (page_type_counts.article || 0) > 0;

  if (!hasPricing && (hasProduct || hasService) && pages_crawled >= 3) {
    actions.push({
      action_type: "site_missing_pricing_page",
      title: "No pricing page found",
      severity: "medium",
      why_it_matters: "Pricing pages rank for high-intent 'cost' and 'pricing' queries and are among the highest-converting pages on any site.",
      technical_reason: "No page was classified as a pricing or cost page during the crawl.",
      expected_impact_range: "Medium-High",
      steps: [
        "Create a dedicated /pricing or /plans page.",
        "Include pricing tiers, a features comparison, and a clear CTA.",
        "Target keywords like '[product] pricing', '[service] cost', 'how much does X cost'.",
      ],
    });
  }

  if (!hasArticle && pages_crawled >= 3) {
    actions.push({
      action_type: "site_no_content_strategy",
      title: "No blog or content pages found",
      severity: "medium",
      why_it_matters: "Content pages drive top-of-funnel traffic and build topical authority, which lifts rankings for your commercial pages too.",
      technical_reason: "No article, blog, or guide pages were discovered during the crawl.",
      expected_impact_range: "Medium",
      steps: [
        "Start a blog or resources section targeting informational keywords in your niche.",
        "Publish 2–4 articles per month consistently.",
        "Internally link from blog posts back to your commercial and service pages.",
      ],
    });
  }

  if (actions.length === 0) return;

  // Remove any stale site-wide actions for this snapshot
  await supabase.from("scc_actions").delete()
    .eq("snapshot_id", snapshotId)
    .is("page_id", null);

  const rows = actions.map((a, i) => ({
    snapshot_id:           snapshotId,
    page_id:               null,
    action_type:           a.action_type,
    title:                 a.title,
    why_it_matters:        a.why_it_matters,
    technical_reason:      a.technical_reason,
    expected_impact_range: a.expected_impact_range,
    steps:                 a.steps,
    severity:              a.severity,
    priority:              i + 1,
    status:                "open",
  }));

  const { error } = await supabase.from("scc_actions").insert(rows);
  if (error) {
    console.error(`[siteWideActions] snapshot=${snapshotId}`, error.message);
  } else {
    console.log(`[siteWideActions] inserted ${rows.length} site-wide actions for snapshot=${snapshotId}`);
  }
}

async function updateJobProgress(jobId, pagesDone, errorsCount) {
  const { error } = await supabase
    .from("scc_crawl_jobs")
    .update({ pages_done: pagesDone, errors_count: errorsCount })
    .eq("id", jobId);

  if (error) {
    console.error(`[updateJobProgress] job=${jobId}`, error.message);
  }
}

// ─────────────────────────────────────────────────────────────────────────────

async function processSinglePage({
  siteId,
  snapshotId,
  jobId,
  url,
  depth,
  seedUrl,
  summaryState,
}) {
  let fetched;
  let fetchError = null;

  const referer = depth > 0 ? seedUrl : null;

  try {
    fetched = await fetchHtml(url, { jobId, referer });
  } catch (err) {
    fetchError = err.message || "Unknown fetch error";

    const pageType = classifyPageTypeFromSignals({ url });
    const pageId = await getOrCreatePage({ siteId, url, pageType });

    await upsertPageSnapshotCrawl({
      snapshotId,
      pageId,
      crawlRow: {
        url,
        final_url: null,
        status_code: null,
        content_type: null,
        load_ms: null,
        title: null,
        meta_description: null,
        canonical_url: null,
        h1_count: null,
        h1_text: null,
        word_count: null,
        robots_meta: null,
        noindex: null,
        indexable: false,
        internal_links_count: 0,
        internal_link_depth: depth,
        page_type: pageType,
        fetch_error: fetchError,
      },
    });

    const structuralScore = 0;
    const visibilityScore = 0;
    const revenueScore = Math.round(getPageIntentWeight(pageType) * 100);
    const paidRiskScore = Math.round(getPageIntentWeight(pageType) * 40);
    const pageOpportunityScore =
      ["pricing", "conversion", "service", "product", "homepage"].includes(pageType) ? 52 : 35;
    const priorityBucket =
      ["pricing", "conversion", "service", "product", "homepage"].includes(pageType) ? "Tier 3" : "Tier 4";

    await upsertPageSnapshotMetrics({
      snapshotId,
      pageId,
      metricsRow: {
        indexable: false,
        canonical_ok: false,
        has_title: false,
        has_meta: false,
        has_h1: false,
        schema_types: [],
        internal_link_depth: depth,
        impressions: 0,
        clicks: 0,
        avg_position: null,
        ctr: null,
        sessions: 0,
        conversions: 0,
        revenue: 0,
        paid_cost: 0,
        paid_clicks: 0,
        paid_conversions: 0,
        paid_revenue: 0,
        structural_score: structuralScore,
        visibility_score: visibilityScore,
        revenue_score: revenueScore,
        paid_risk_score: paidRiskScore,
        page_opportunity_score: pageOpportunityScore,
        priority_bucket: priorityBucket,
      },
    });

    const actions = buildActions({
      pageType,
      statusCode: 500,
      indexable: false,
      canonicalOk: false,
      hasTitle: false,
      hasMeta: false,
      hasH1: false,
      wordCount: 0,
      title: "",
      metaDescription: "",
      h1Count: 0,
      pageOpportunityScore,
      structuralScore,
      visibilityScore,
      revenueScore,
      internalLinkDepth: depth,
      loadMs: null,
    });

    await replaceActions({ snapshotId, pageId, actions });

    if (summaryState) {
      summaryState.errors_count += 1;
      registerSummaryPage(summaryState, {
        url,
        pageType,
        structuralScore,
        visibilityScore,
        revenueScore,
        paidRiskScore,
        pageOpportunityScore,
        priorityBucket,
        indexable: false,
        canonicalOk: false,
        hasTitle: false,
        hasMeta: false,
        hasH1: false,
        wordCount: 0,
        loadMs: null,
        internalLinkDepth: depth,
      });
    }

    return {
      stored: true,
      url,
      pageId,
      pageType,
      links: [],
      fetchError,
    };
  }

  const finalUrl = fetched.finalUrl || url;
  const effectiveUrl = normalizeUrl(finalUrl) || normalizeUrl(url) || url;

  if (!fetched.contentType.includes("text/html")) {
    const pageType = classifyPageTypeFromSignals({ url: effectiveUrl });
    const pageId = await getOrCreatePage({ siteId, url: effectiveUrl, pageType });

    await upsertPageSnapshotCrawl({
      snapshotId,
      pageId,
      crawlRow: {
        url,
        final_url: effectiveUrl,
        status_code: fetched.status,
        content_type: fetched.contentType,
        load_ms: fetched.loadMs,
        title: null,
        meta_description: null,
        canonical_url: null,
        h1_count: null,
        h1_text: null,
        word_count: null,
        robots_meta: null,
        noindex: false,
        indexable: false,
        internal_links_count: 0,
        internal_link_depth: depth,
        page_type: pageType,
        fetch_error: null,
      },
    });

    const structuralScore = 0;
    const visibilityScore = 0;
    const revenueScore = Math.round(getPageIntentWeight(pageType) * 100);
    const paidRiskScore = 0;
    const pageOpportunityScore = 10;
    const priorityBucket = "Tier 4";

    await upsertPageSnapshotMetrics({
      snapshotId,
      pageId,
      metricsRow: {
        indexable: false,
        canonical_ok: false,
        has_title: false,
        has_meta: false,
        has_h1: false,
        schema_types: [],
        internal_link_depth: depth,
        impressions: 0,
        clicks: 0,
        avg_position: null,
        ctr: null,
        sessions: 0,
        conversions: 0,
        revenue: 0,
        paid_cost: 0,
        paid_clicks: 0,
        paid_conversions: 0,
        paid_revenue: 0,
        structural_score: structuralScore,
        visibility_score: visibilityScore,
        revenue_score: revenueScore,
        paid_risk_score: paidRiskScore,
        page_opportunity_score: pageOpportunityScore,
        priority_bucket: priorityBucket,
      },
    });

    await replaceActions({ snapshotId, pageId, actions: [] });

    if (summaryState) {
      registerSummaryPage(summaryState, {
        url: effectiveUrl,
        pageType,
        structuralScore,
        visibilityScore,
        revenueScore,
        paidRiskScore,
        pageOpportunityScore,
        priorityBucket,
        indexable: false,
        canonicalOk: false,
        hasTitle: false,
        hasMeta: false,
        hasH1: false,
        wordCount: 0,
        loadMs: fetched.loadMs,
        internalLinkDepth: depth,
      });
    }

    return {
      stored: true,
      url: effectiveUrl,
      pageId,
      pageType,
      links: [],
      fetchError: null,
    };
  }

  const extracted = extractSeoData(
    fetched.html,
    effectiveUrl,
    fetched.status,
    fetched.contentType,
    fetched.loadMs,
    depth,
    seedUrl
  );

  const pageType = extracted.pageType;
  const links = extracted.internalLinks;
  const pageId = await getOrCreatePage({ siteId, url: effectiveUrl, pageType });

  const canonicalOk = evaluateCanonicalOk(effectiveUrl, extracted.canonicalUrl);
  const hasTitle = Boolean(extracted.title);
  const hasMeta = Boolean(extracted.metaDescription);
  const hasH1 = extracted.h1Count > 0;

  const structuralScore = computeStructuralScore({
    indexable: extracted.indexable,
    canonicalOk,
    hasTitle,
    hasMeta,
    hasH1,
    wordCount: extracted.wordCount,
    statusCode: extracted.statusCode,
    pageType,
    loadMs: extracted.loadMs,
    schemaTypes: extracted.schemaTypes,
  });

  const visibilityScore = computeVisibilityScore({
    indexable: extracted.indexable,
    structuralScore,
    pageType,
    statusCode: extracted.statusCode,
    internalLinkDepth: depth,
    noindex: extracted.noindex,
    loadMs: extracted.loadMs,
  });

  const revenueScore = computeRevenueScore(pageType);

  const paidRiskScore = computePaidRiskScore({
    pageType,
    indexable: extracted.indexable,
    structuralScore,
    visibilityScore,
    loadMs: extracted.loadMs,
  });

  const pageOpportunityScore = computePageOpportunityScore({
    pageType,
    structuralScore,
    visibilityScore,
    revenueScore,
    indexable: extracted.indexable,
    statusCode: extracted.statusCode,
    wordCount: extracted.wordCount,
    internalLinkDepth: depth,
    canonicalOk,
    hasTitle,
    hasMeta,
    hasH1,
  });

  const priorityBucket = computePriorityBucket(pageOpportunityScore, revenueScore, pageType);

  console.log(
    `[page scored] ${effectiveUrl} -> ${pageType} structural=${structuralScore} visibility=${visibilityScore} revenue=${revenueScore} opp=${pageOpportunityScore}`
  );

  await upsertPageSnapshotCrawl({
    snapshotId,
    pageId,
    crawlRow: {
      url,
      final_url: effectiveUrl,
      status_code: extracted.statusCode,
      content_type: extracted.contentType,
      load_ms: extracted.loadMs,
      title: extracted.title || null,
      meta_description: extracted.metaDescription || null,
      canonical_url: extracted.canonicalUrl,
      h1_count: extracted.h1Count,
      h1_text: extracted.h1Text,
      word_count: extracted.wordCount,
      robots_meta: extracted.robotsMeta,
      noindex: extracted.noindex,
      indexable: extracted.indexable,
      internal_links_count: links.length,
      internal_link_depth: depth,
      page_type: pageType,
      fetch_error: null,
    },
  });

  await upsertPageSnapshotMetrics({
    snapshotId,
    pageId,
    metricsRow: {
      indexable: extracted.indexable,
      canonical_ok: canonicalOk,
      has_title: hasTitle,
      has_meta: hasMeta,
      has_h1: hasH1,
      schema_types: extracted.schemaTypes,
      internal_link_depth: depth,
      impressions: 0,
      clicks: 0,
      avg_position: null,
      ctr: null,
      sessions: 0,
      conversions: 0,
      revenue: 0,
      paid_cost: 0,
      paid_clicks: 0,
      paid_conversions: 0,
      paid_revenue: 0,
      structural_score: structuralScore,
      visibility_score: visibilityScore,
      revenue_score: revenueScore,
      paid_risk_score: paidRiskScore,
      page_opportunity_score: pageOpportunityScore,
      priority_bucket: priorityBucket,
    },
  });

  const actions = buildActions({
    pageType,
    statusCode: extracted.statusCode,
    indexable: extracted.indexable,
    canonicalOk,
    hasTitle,
    hasMeta,
    hasH1,
    wordCount: extracted.wordCount,
    title: extracted.title,
    metaDescription: extracted.metaDescription,
    h1Count: extracted.h1Count,
    pageOpportunityScore,
    structuralScore,
    visibilityScore,
    revenueScore,
    internalLinkDepth: depth,
    loadMs: extracted.loadMs,
  });

  await replaceActions({ snapshotId, pageId, actions });

  if (summaryState) {
    registerSummaryPage(summaryState, {
      url: effectiveUrl,
      pageType,
      structuralScore,
      visibilityScore,
      revenueScore,
      paidRiskScore,
      pageOpportunityScore,
      priorityBucket,
      indexable: extracted.indexable,
      canonicalOk,
      hasTitle,
      hasMeta,
      hasH1,
      wordCount: extracted.wordCount,
      loadMs: extracted.loadMs,
      internalLinkDepth: depth,
      locationSignals: extracted.locationSignals,
      bodyTextSnippet: extracted.bodyTextSnippet,
    });
  }

  return {
    stored: true,
    url: effectiveUrl,
    pageId,
    pageType,
    links,
    fetchError: null,
  };
}

async function markSnapshotRunning(snapshotId) {
  const { error } = await supabase
    .from("scc_snapshots")
    .update({
      status: "running",
      started_at: nowIso(),
      progress_step: "crawling_pages",
      error_stage: null,
      error_message: null,
    })
    .eq("id", snapshotId);

  if (error) console.error(`[snapshot running update] snapshot=${snapshotId}`, error.message);
}

async function fetchGscData(siteId, snapshotId) {
  const fnUrl = `${SUPABASE_URL}/functions/v1/gsc-fetch-data`;
  const res = await fetch(fnUrl, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
    },
    body: JSON.stringify({ site_id: siteId, snapshot_id: snapshotId }),
  });
  const data = await res.json();
  if (data.ok) {
    console.log(`[gsc fetch] updated ${data.updated}/${data.pages} pages for snapshot=${snapshotId}`);
  } else {
    console.log(`[gsc fetch] skipped (${data.reason || "unknown"}) for snapshot=${snapshotId}`);
  }
}

async function fetchPsiData(snapshotId) {
  const GOOGLE_API_KEY = process.env.GOOGLE_API_KEY;
  if (!GOOGLE_API_KEY) {
    console.log("[psi fetch] GOOGLE_API_KEY not set, skipping");
    return;
  }

  // Get all pages for this snapshot
  const { data: metrics, error } = await supabase
    .from("scc_page_snapshot_metrics")
    .select("page_id, scc_pages(url)")
    .eq("snapshot_id", snapshotId);

  if (error || !metrics?.length) {
    console.log(`[psi fetch] no pages found for snapshot=${snapshotId}`);
    return;
  }

  console.log(`[psi fetch] fetching PSI for ${metrics.length} pages`);

  // Process pages in parallel batches of 3 to avoid overwhelming PSI API
  const PSI_BATCH_SIZE = 3;
  for (let i = 0; i < metrics.length; i += PSI_BATCH_SIZE) {
    const batch = metrics.slice(i, i + PSI_BATCH_SIZE);
    await Promise.all(batch.map(async (m) => {
    const url = m.scc_pages?.url;
    if (!url) return;

    try {
      // Fetch mobile and desktop PSI in parallel
      const [mobileRes, desktopRes] = await Promise.all([
        axios.get("https://www.googleapis.com/pagespeedonline/v5/runPagespeed", {
          params: { url, key: GOOGLE_API_KEY, strategy: "mobile" },
          timeout: 30000,
          validateStatus: () => true,
        }).catch(() => null),
        axios.get("https://www.googleapis.com/pagespeedonline/v5/runPagespeed", {
          params: { url, key: GOOGLE_API_KEY, strategy: "desktop" },
          timeout: 30000,
          validateStatus: () => true,
        }).catch(() => null),
      ]);

      const mobileScore = mobileRes?.data?.lighthouseResult?.categories?.performance?.score;
      const desktopScore = desktopRes?.data?.lighthouseResult?.categories?.performance?.score;

      // Extract Lighthouse lab data (from mobile run)
      const audits = mobileRes?.data?.lighthouseResult?.audits || {};
      const lcp  = audits["largest-contentful-paint"]?.numericValue;
      const cls  = audits["cumulative-layout-shift"]?.numericValue;
      const inp  = audits["interaction-to-next-paint"]?.numericValue;
      const fcp  = audits["first-contentful-paint"]?.numericValue;
      const ttfb = audits["time-to-first-byte"]?.numericValue;

      // Extract CrUX field data (real users, from loadingExperience)
      const fieldData = mobileRes?.data?.loadingExperience?.metrics || {};
      const cruxLcp    = fieldData["LARGEST_CONTENTFUL_PAINT_MS"]?.percentile;
      const cruxCls    = fieldData["CUMULATIVE_LAYOUT_SHIFT_SCORE"]?.percentile;
      const cruxInp    = fieldData["INTERACTION_TO_NEXT_PAINT"]?.percentile;
      const cruxFcp    = fieldData["FIRST_CONTENTFUL_PAINT_MS"]?.percentile;
      const cruxTtfb   = fieldData["EXPERIMENTAL_TIME_TO_FIRST_BYTE"]?.percentile;
      const cruxLcpRating = fieldData["LARGEST_CONTENTFUL_PAINT_MS"]?.category;
      const cruxClsRating = fieldData["CUMULATIVE_LAYOUT_SHIFT_SCORE"]?.category;
      const cruxInpRating = fieldData["INTERACTION_TO_NEXT_PAINT"]?.category;

      const update = {};
      if (mobileScore  != null) update.performance_score_mobile  = Math.round(mobileScore  * 100);
      if (desktopScore != null) update.performance_score_desktop = Math.round(desktopScore * 100);
      if (lcp  != null) update.lcp_ms    = Math.round(lcp);
      if (cls  != null) update.cls_score = parseFloat(cls.toFixed(4));
      if (inp  != null) update.inp_ms    = Math.round(inp);
      if (fcp  != null) update.fcp_ms    = Math.round(fcp);
      if (ttfb != null) update.ttfb_ms   = Math.round(ttfb);
      // CrUX: CLS percentile is stored as integer x1000 by Google — normalise to decimal
      if (cruxLcp  != null) update.crux_lcp_ms    = cruxLcp;
      if (cruxCls  != null) update.crux_cls_score = parseFloat((cruxCls / 100).toFixed(4));
      if (cruxInp  != null) update.crux_inp_ms    = cruxInp;
      if (cruxFcp  != null) update.crux_fcp_ms    = cruxFcp;
      if (cruxTtfb != null) update.crux_ttfb_ms   = cruxTtfb;
      if (cruxLcpRating) update.crux_lcp_rating = cruxLcpRating;
      if (cruxClsRating) update.crux_cls_rating = cruxClsRating;
      if (cruxInpRating) update.crux_inp_rating = cruxInpRating;

      if (Object.keys(update).length > 0) {
        await supabase
          .from("scc_page_snapshot_metrics")
          .update(update)
          .eq("snapshot_id", snapshotId)
          .eq("page_id", m.page_id);

        console.log(
          `[psi fetch] ${url} mobile=${update.performance_score_mobile ?? "n/a"} desktop=${update.performance_score_desktop ?? "n/a"} lcp=${update.lcp_ms ?? "n/a"}ms`
        );
      }
    } catch (err) {
      console.warn(`[psi fetch] error for ${url}: ${err.message}`);
    }
    })); // end batch map
  } // end batch loop

  console.log(`[psi fetch] done for snapshot=${snapshotId}`);
}

async function markSnapshotFinished(snapshotId) {
  const { error } = await supabase
    .from("scc_snapshots")
    .update({
      status: "completed",
      finished_at: nowIso(),
      progress_step: "crawl_complete",
      error_stage: null,
      error_message: null,
    })
    .eq("id", snapshotId);

  if (error) console.error(`[snapshot completed update] snapshot=${snapshotId}`, error.message);
}

async function markSnapshotFailed(snapshotId, stage, message) {
  const { error } = await supabase
    .from("scc_snapshots")
    .update({
      status: "failed",
      finished_at: nowIso(),
      error_stage: stage,
      error_message: message?.slice(0, 1000) || "Unknown error",
    })
    .eq("id", snapshotId);

  if (error) console.error(`[snapshot failed update] snapshot=${snapshotId}`, error.message);
}

async function heartbeat(jobId) {
  const { error } = await supabase.rpc("scc_job_heartbeat", {
    p_job_id: jobId,
  });

  if (error) {
    console.error(`[job heartbeat] job=${jobId}`, error.message);
  }
}

async function completeJob(jobId, status, errorMessage = null) {
  const payload = {
    p_job_id: jobId,
    p_status: status,
  };

  if (errorMessage) {
    payload.p_error = String(errorMessage).slice(0, 1000);
  }

  const { error } = await supabase.rpc("scc_complete_crawl_job", payload);

  if (error) {
    console.error(`[job complete] job=${jobId} status=${status}`, error.message);
  }
}

async function runCrawlJob(job) {
  const jobId = job.id;
  const siteId = job.site_id;
  const snapshotId = job.snapshot_id;
  const seedUrl = normalizeUrl(job.seed_url);
  const maxPages = Number(job.max_pages || 8);
  const maxDepth = Number(job.max_depth || 1);
  const crawlDelayMs = Number(job.crawl_delay_ms || 0);
  const respectRobots = Boolean(job.respect_robots);
  const renderJs = Boolean(job.render_js);

  if (!seedUrl) throw new Error("Invalid seed_url on crawl job");

  console.log(
    `[job start] id=${jobId} seed=${seedUrl} maxPages=${maxPages} maxDepth=${maxDepth} crawlDelayMs=${crawlDelayMs} respectRobots=${respectRobots} renderJs=${renderJs}`
  );

  let pagesDone = 0;
  let errorsCount = 0;

  const seen = new Set();
  const queued = new Set();
  const homepageNavSet = new Set();
  const siblingTypeCounts = {};
  const queue = [];
  const queueState = createQueueState();
  const summaryState = createSnapshotSummaryState(seedUrl);

  const heartbeatTimer = setInterval(() => {
    heartbeat(jobId);
  }, HEARTBEAT_MS);

  try {
    await markSnapshotRunning(snapshotId);
    await heartbeat(jobId);

    const homepageResult = await processSinglePage({
      siteId,
      snapshotId,
      jobId,
      url: seedUrl,
      depth: 0,
      seedUrl,
      summaryState,
    });

    seen.add(homepageResult.url);
    pagesDone += 1;
    if (homepageResult.fetchError) errorsCount += 1;

    registerSelectedPage(
      queueState,
      homepageResult.pageType,
      getUrlFamily(homepageResult.url, homepageResult.pageType)
    );

    await updateJobProgress(jobId, pagesDone, errorsCount);

    if (pagesDone >= maxPages) {
      summaryState.site_type = "mixed";
      const summaryJson = buildSnapshotSummary(summaryState);
      await updateSnapshotSummary(snapshotId, summaryJson);
      await generateSiteWideActions(snapshotId, summaryState);

      await markSnapshotFinished(snapshotId);
      await completeJob(jobId, "completed");
      deleteCookieJar(jobId);
      console.log(`[job done] id=${jobId} pages=${pagesDone}`);
      return;
    }

    let homepageLinks = homepageResult.links || [];
    try {
      const homeFetch = await fetchHtml(seedUrl, { jobId });
      if (homeFetch.contentType.includes("text/html")) {
        const $ = cheerio.load(homeFetch.html || "");
        const navLinks = extractNavLinks($, seedUrl);

        navLinks.forEach((link) => homepageNavSet.add(link.url));

        const merged = new Map();
        [...homepageLinks, ...navLinks].forEach((link) => {
          if (!merged.has(link.url)) merged.set(link.url, link);
        });
        homepageLinks = [...merged.values()];
      }
    } catch (err) {
      console.warn("[homepage nav extraction warning]", err.message);
    }

    const siteType = inferSiteTypeFromHomepage(homepageLinks);
    summaryState.site_type = siteType;
    console.log(`[site type inferred] ${siteType}`);

    for (const link of homepageLinks) {
      if (seen.has(link.url) || queued.has(link.url)) continue;
      if (!sameHost(link.url, seedUrl)) continue;
      if (!isLikelyHtmlUrl(link.url)) continue;

      const priority = buildPriorityScore({
        candidateUrl: link.url,
        anchorText: link.anchorText,
        depth: 1,
        parentPageType: "homepage",
        siteType,
        homepageNavSet,
        siblingTypeCounts,
        queueState,
        maxPages,
      });

      if (priority.score < 8) continue;

      queue.push({
        url: link.url,
        depth: 1,
        score: priority.score,
        pageType: priority.pageType,
        familyKey: priority.familyKey,
        anchorText: link.anchorText || "",
        parentPageType: "homepage",
      });

      queued.add(link.url);
      siblingTypeCounts[priority.pageType] =
        (siblingTypeCounts[priority.pageType] || 0) + 1;
      registerEnqueuedCandidate(queueState, priority.pageType, priority.familyKey);
    }

    while (queue.length > 0 && pagesDone < maxPages) {
      queue.sort((a, b) => b.score - a.score);
      const next = queue.shift();

      if (!next) break;
      if (seen.has(next.url)) continue;
      if (next.depth > maxDepth) continue;

      if (crawlDelayMs > 0) await sleep(crawlDelayMs);

      try {
        const pageResult = await processSinglePage({
          siteId,
          snapshotId,
          jobId,
          url: next.url,
          depth: next.depth,
          seedUrl,
          summaryState,
        });

        seen.add(pageResult.url);
        pagesDone += 1;
        if (pageResult.fetchError) errorsCount += 1;

        registerSelectedPage(
          queueState,
          pageResult.pageType,
          getUrlFamily(pageResult.url, pageResult.pageType)
        );

        await updateJobProgress(jobId, pagesDone, errorsCount);

        if (pagesDone >= maxPages) break;

        for (const link of pageResult.links || []) {
          if (seen.has(link.url) || queued.has(link.url)) continue;
          if (!sameHost(link.url, seedUrl)) continue;
          if (!isLikelyHtmlUrl(link.url)) continue;

          const nextDepth = next.depth + 1;
          if (nextDepth > maxDepth) continue;

          const priority = buildPriorityScore({
            candidateUrl: link.url,
            anchorText: link.anchorText,
            depth: nextDepth,
            parentPageType: pageResult.pageType,
            siteType,
            homepageNavSet,
            siblingTypeCounts,
            queueState,
            maxPages,
          });

          if (priority.score < 8) continue;

          queue.push({
            url: link.url,
            depth: nextDepth,
            score: priority.score,
            pageType: priority.pageType,
            familyKey: priority.familyKey,
            anchorText: link.anchorText || "",
            parentPageType: pageResult.pageType,
          });

          queued.add(link.url);
          siblingTypeCounts[priority.pageType] =
            (siblingTypeCounts[priority.pageType] || 0) + 1;
          registerEnqueuedCandidate(queueState, priority.pageType, priority.familyKey);
        }
      } catch (err) {
        errorsCount += 1;
        summaryState.errors_count += 1;
        console.error(`[page error] ${next.url}`, err.message);
        await updateJobProgress(jobId, pagesDone, errorsCount);
      }
    }

    summaryState.errors_count = errorsCount;
    const summaryJson = buildSnapshotSummary(summaryState);
    await updateSnapshotSummary(snapshotId, summaryJson);
    await generateSiteWideActions(snapshotId, summaryState);

    // Enrich with Google Search Console data (non-blocking)
    fetchGscData(siteId, snapshotId).catch((err) =>
      console.warn(`[gsc fetch] skipped: ${err.message}`)
    );

    // Run PSI and money engine in parallel BEFORE marking snapshot finished
    // so all data is ready when the frontend loads results
    await Promise.all([
      fetchPsiData(snapshotId).catch((err) =>
        console.warn(`[psi fetch] skipped: ${err.message}`)
      ),
      runMoneyEngine(siteId, snapshotId, seedUrl, summaryState).catch((err) =>
        console.warn(`[money engine] skipped: ${err.message}`)
      ),
    ]);

    await markSnapshotFinished(snapshotId);
    await completeJob(jobId, "completed");
    console.log(`[job done] id=${jobId} pages=${pagesDone} errors=${errorsCount}`);
  } catch (err) {
    console.error(`[job failed] id=${jobId}`, err);
    await markSnapshotFailed(snapshotId, "worker_run", err.message || "Unknown crawl error");
    await completeJob(jobId, "failed", err.message || "Unknown crawl error");
    throw err;
  } finally {
    clearInterval(heartbeatTimer);
    deleteCookieJar(jobId);

    const { error } = await supabase
      .from("scc_crawl_jobs")
      .update({
        pages_done: pagesDone,
        errors_count: errorsCount,
      })
      .eq("id", jobId);

    if (error) {
      console.error(`[job counters update] job=${jobId}`, error.message);
    }
  }
}

// =============================================
// MONEY CALCULATION ENGINE
// =============================================

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const GOOGLE_CSE_ID = process.env.GOOGLE_CUSTOM_SEARCH_ENGINE_ID;
const GOOGLE_API_KEY_MONEY = process.env.GOOGLE_API_KEY;

const CTR_TABLE = {
  1: 28.5, 2: 15.7, 3: 11.0, 4: 8.0, 5: 7.2,
  6: 5.1, 7: 4.0, 8: 3.2, 9: 2.8, 10: 2.5,
};
function ctrAtPosition(pos) {
  if (pos <= 0) return CTR_TABLE[1];
  if (pos > 20) return 0.4;
  if (pos > 10) return 1.0;
  return CTR_TABLE[pos] || 1.0;
}

const BENCHMARKS = {
  US: {
    ecommerce:         { convRate: 0.020, avgOrder: 85,   valuePerVisitor: 1.50 },
    saas:              { convRate: 0.035, avgOrder: 99,   valuePerVisitor: 3.00 },
    legal:             { convRate: 0.030, avgOrder: 2500, valuePerVisitor: 62.50 },
    "real estate":     { convRate: 0.010, avgOrder: 8000, valuePerVisitor: 80.00 },
    healthcare:        { convRate: 0.028, avgOrder: 250,  valuePerVisitor: 6.25 },
    financial:         { convRate: 0.022, avgOrder: 500,  valuePerVisitor: 10.00 },
    "home services":   { convRate: 0.040, avgOrder: 350,  valuePerVisitor: 13.00 },
    restaurant:        { convRate: 0.045, avgOrder: 45,   valuePerVisitor: 1.80 },
    education:         { convRate: 0.022, avgOrder: 400,  valuePerVisitor: 8.00 },
    travel:            { convRate: 0.015, avgOrder: 800,  valuePerVisitor: 12.00 },
    auto:              { convRate: 0.030, avgOrder: 600,  valuePerVisitor: 15.00 },
    beauty:            { convRate: 0.028, avgOrder: 120,  valuePerVisitor: 3.00 },
    service:           { convRate: 0.025, avgOrder: 500,  valuePerVisitor: 8.00 },
    default:           { convRate: 0.020, avgOrder: 300,  valuePerVisitor: 5.00 },
  },
  UK: {
    ecommerce:         { convRate: 0.020, avgOrder: 65,   valuePerVisitor: 1.20 },
    saas:              { convRate: 0.035, avgOrder: 79,   valuePerVisitor: 2.40 },
    legal:             { convRate: 0.030, avgOrder: 2000, valuePerVisitor: 50.00 },
    "real estate":     { convRate: 0.010, avgOrder: 6000, valuePerVisitor: 60.00 },
    healthcare:        { convRate: 0.028, avgOrder: 200,  valuePerVisitor: 5.00 },
    financial:         { convRate: 0.022, avgOrder: 400,  valuePerVisitor: 8.00 },
    "home services":   { convRate: 0.040, avgOrder: 280,  valuePerVisitor: 10.50 },
    restaurant:        { convRate: 0.045, avgOrder: 35,   valuePerVisitor: 1.40 },
    education:         { convRate: 0.022, avgOrder: 320,  valuePerVisitor: 6.40 },
    travel:            { convRate: 0.015, avgOrder: 650,  valuePerVisitor: 9.75 },
    auto:              { convRate: 0.030, avgOrder: 480,  valuePerVisitor: 12.00 },
    beauty:            { convRate: 0.028, avgOrder: 95,   valuePerVisitor: 2.40 },
    service:           { convRate: 0.025, avgOrder: 400,  valuePerVisitor: 6.50 },
    default:           { convRate: 0.020, avgOrder: 250,  valuePerVisitor: 4.00 },
  },
  // India (INR)
  India: {
    ecommerce:         { convRate: 0.015, avgOrder: 2500, valuePerVisitor: 37.50 },
    saas:              { convRate: 0.030, avgOrder: 2000, valuePerVisitor: 60.00 },
    legal:             { convRate: 0.025, avgOrder: 25000,valuePerVisitor: 625.00 },
    "real estate":     { convRate: 0.008, avgOrder: 500000,valuePerVisitor: 4000.00 },
    healthcare:        { convRate: 0.025, avgOrder: 3000, valuePerVisitor: 75.00 },
    restaurant:        { convRate: 0.040, avgOrder: 600,  valuePerVisitor: 24.00 },
    education:         { convRate: 0.020, avgOrder: 5000, valuePerVisitor: 100.00 },
    travel:            { convRate: 0.012, avgOrder: 15000,valuePerVisitor: 180.00 },
    service:           { convRate: 0.022, avgOrder: 5000, valuePerVisitor: 110.00 },
    default:           { convRate: 0.018, avgOrder: 3000, valuePerVisitor: 54.00 },
  },
  // Australia (AUD)
  Australia: {
    ecommerce:         { convRate: 0.020, avgOrder: 120,  valuePerVisitor: 2.40 },
    saas:              { convRate: 0.035, avgOrder: 120,  valuePerVisitor: 4.20 },
    legal:             { convRate: 0.028, avgOrder: 3500, valuePerVisitor: 98.00 },
    "real estate":     { convRate: 0.010, avgOrder: 12000,valuePerVisitor: 120.00 },
    healthcare:        { convRate: 0.028, avgOrder: 350,  valuePerVisitor: 9.80 },
    "home services":   { convRate: 0.038, avgOrder: 450,  valuePerVisitor: 17.10 },
    restaurant:        { convRate: 0.042, avgOrder: 60,   valuePerVisitor: 2.52 },
    education:         { convRate: 0.020, avgOrder: 500,  valuePerVisitor: 10.00 },
    travel:            { convRate: 0.015, avgOrder: 1200, valuePerVisitor: 18.00 },
    service:           { convRate: 0.025, avgOrder: 600,  valuePerVisitor: 15.00 },
    default:           { convRate: 0.020, avgOrder: 400,  valuePerVisitor: 8.00 },
  },
  // Canada (CAD)
  Canada: {
    ecommerce:         { convRate: 0.020, avgOrder: 95,   valuePerVisitor: 1.90 },
    saas:              { convRate: 0.035, avgOrder: 110,  valuePerVisitor: 3.85 },
    legal:             { convRate: 0.028, avgOrder: 3000, valuePerVisitor: 84.00 },
    "real estate":     { convRate: 0.010, avgOrder: 10000,valuePerVisitor: 100.00 },
    healthcare:        { convRate: 0.028, avgOrder: 300,  valuePerVisitor: 8.40 },
    restaurant:        { convRate: 0.042, avgOrder: 50,   valuePerVisitor: 2.10 },
    education:         { convRate: 0.020, avgOrder: 450,  valuePerVisitor: 9.00 },
    service:           { convRate: 0.025, avgOrder: 550,  valuePerVisitor: 13.75 },
    default:           { convRate: 0.020, avgOrder: 350,  valuePerVisitor: 7.00 },
  },
  // Europe (EUR)
  Europe: {
    ecommerce:         { convRate: 0.018, avgOrder: 75,   valuePerVisitor: 1.35 },
    saas:              { convRate: 0.032, avgOrder: 90,   valuePerVisitor: 2.88 },
    legal:             { convRate: 0.028, avgOrder: 2200, valuePerVisitor: 61.60 },
    "real estate":     { convRate: 0.010, avgOrder: 7000, valuePerVisitor: 70.00 },
    healthcare:        { convRate: 0.026, avgOrder: 220,  valuePerVisitor: 5.72 },
    restaurant:        { convRate: 0.040, avgOrder: 40,   valuePerVisitor: 1.60 },
    education:         { convRate: 0.020, avgOrder: 350,  valuePerVisitor: 7.00 },
    travel:            { convRate: 0.015, avgOrder: 700,  valuePerVisitor: 10.50 },
    service:           { convRate: 0.022, avgOrder: 450,  valuePerVisitor: 9.90 },
    default:           { convRate: 0.018, avgOrder: 280,  valuePerVisitor: 5.00 },
  },
};

function detectMarket(seedUrl) {
  try {
    const url = new URL(seedUrl.startsWith("http") ? seedUrl : `https://${seedUrl}`);
    const h = url.hostname.toLowerCase();
    // UK — standard + city/regional TLDs
    if (
      h.endsWith(".co.uk") || h.endsWith(".org.uk") || h.endsWith(".me.uk") ||
      h.endsWith(".net.uk") || h.endsWith(".ltd.uk") || h.endsWith(".plc.uk") ||
      h.endsWith(".uk") ||
      h.endsWith(".london") || h.endsWith(".wales") || h.endsWith(".cymru") ||
      h.endsWith(".scot") || h.endsWith(".manchester") || h.endsWith(".birmingham") ||
      h.endsWith(".glasgow") || h.endsWith(".leeds") || h.endsWith(".liverpool") ||
      h.endsWith(".sheffield") || h.endsWith(".bristol") || h.endsWith(".edinburgh")
    )
      return { market: "UK", currency: "GBP", symbol: "£", confidence: 90 };
    // India
    if (h.endsWith(".in") || h.endsWith(".co.in") || h.endsWith(".ind.in"))
      return { market: "India", currency: "INR", symbol: "₹", confidence: 85 };
    // Australia
    if (h.endsWith(".com.au") || h.endsWith(".net.au") || h.endsWith(".org.au") || h.endsWith(".au"))
      return { market: "Australia", currency: "AUD", symbol: "A$", confidence: 88 };
    // Canada
    if (h.endsWith(".ca"))
      return { market: "Canada", currency: "CAD", symbol: "C$", confidence: 85 };
    // Germany
    if (h.endsWith(".de"))
      return { market: "Europe", currency: "EUR", symbol: "€", confidence: 85 };
    // France
    if (h.endsWith(".fr"))
      return { market: "Europe", currency: "EUR", symbol: "€", confidence: 85 };
    // Europe
    if (h.endsWith(".eu") || h.endsWith(".es") || h.endsWith(".it") || h.endsWith(".nl") || h.endsWith(".be") || h.endsWith(".pl") || h.endsWith(".se") || h.endsWith(".no") || h.endsWith(".dk") || h.endsWith(".fi") || h.endsWith(".at") || h.endsWith(".ch") || h.endsWith(".pt"))
      return { market: "Europe", currency: "EUR", symbol: "€", confidence: 80 };
    // New Zealand
    if (h.endsWith(".co.nz") || h.endsWith(".nz"))
      return { market: "Australia", currency: "AUD", symbol: "A$", confidence: 75 };
    // South Africa
    if (h.endsWith(".co.za") || h.endsWith(".za"))
      return { market: "US", currency: "ZAR", symbol: "R", confidence: 75 };
    // Singapore / Malaysia / Hong Kong - use USD as proxy
    if (h.endsWith(".sg") || h.endsWith(".com.sg") || h.endsWith(".com.my") || h.endsWith(".hk"))
      return { market: "US", currency: "USD", symbol: "$", confidence: 65 };
    // Default: US
    return { market: "US", currency: "USD", symbol: "$", confidence: 60 };
  } catch {
    return { market: "US", currency: "USD", symbol: "$", confidence: 40 };
  }
}

function classifyIndustry(pageTypeCounts, siteType, seedUrl) {
  const domain = (seedUrl || "").toLowerCase();
  const types = Object.keys(pageTypeCounts || {});

  if (siteType === "ecommerce" || types.includes("product") || types.includes("category")) return "ecommerce";
  if (types.includes("pricing") || /saas|software|app\./.test(domain)) return "saas";
  if (/law|legal|solicitor|attorney|barrister/.test(domain)) return "legal";
  if (/restaurant|cafe|bistro|diner|food/.test(domain)) return "restaurant";
  if (/estate|property|homes|realty|realtor/.test(domain)) return "real estate";
  if (/health|medical|clinic|dental|doctor|physio/.test(domain)) return "healthcare";
  if (/finance|mortgage|insurance|invest|accountan/.test(domain)) return "financial";
  if (/hotel|travel|holiday|tour|flight/.test(domain)) return "travel";
  if (/school|college|university|course|academy|learn/.test(domain)) return "education";
  if (/plumb|electric|clean|pest|garden|landscap|roof|build/.test(domain)) return "home services";
  if (/car|auto|vehicle|garage|tyre|tire|mechanic/.test(domain)) return "auto";
  if (/salon|spa|beauty|hair|nail|skin/.test(domain)) return "beauty";
  if (types.includes("service")) return "service";
  return "service";
}

// ─── Google Natural Language API ─────────────────────────────────────────────

async function callNaturalLanguageAPI(text) {
  if (!GOOGLE_API_KEY_MONEY) return null;
  const content = (text || "").trim();
  if (content.length < 100) return null;
  try {
    const res = await axios.post(
      `https://language.googleapis.com/v1/documents:annotateText?key=${GOOGLE_API_KEY_MONEY}`,
      {
        document: { type: "PLAIN_TEXT", content: content.slice(0, 5000) },
        features: {
          classifyText: content.length >= 250, // min length required
          extractEntities: true,
          extractDocumentSentiment: true,
        },
        encodingType: "UTF8",
      },
      { timeout: 10000 }
    );
    return res.data;
  } catch (e) {
    console.warn("[nl api] failed:", e.message);
    return null;
  }
}

/**
 * Map Google NL IAB categories → our industry keys
 */
function mapNLCategoriesToIndustry(categories) {
  if (!categories || !categories.length) return null;
  const top = [...categories].sort((a, b) => (b.confidence || 0) - (a.confidence || 0))[0];
  const name = (top.name || "").toLowerCase();

  if (/restaurant|food & drink|dining|cafe|bakery|bar & nightlife|pub/.test(name)) return "restaurant";
  if (/software|saas|internet & telecom|computers|technology/.test(name)) return "saas";
  if (/shop|retail|apparel|e-commerce|consumer electronics|gifts/.test(name)) return "ecommerce";
  if (/law|legal|attorney|courts|paralegal/.test(name)) return "legal";
  if (/health|medical|pharmacy|dental|fitness|wellness/.test(name)) return "healthcare";
  if (/real estate|property|homes for sale|mortgage/.test(name)) return "real estate";
  if (/finance|insurance|banking|investing|accounting/.test(name)) return "financial";
  if (/hotel|travel|tourism|vacation|flights|accommodation/.test(name)) return "travel";
  if (/education|school|university|college|training|learning/.test(name)) return "education";
  if (/beauty|salon|spa|hair|cosmetics/.test(name)) return "beauty";
  if (/automotive|car|vehicle|auto parts/.test(name)) return "auto";
  if (/home & garden|construction|plumbing|roofing|cleaning/.test(name)) return "home services";
  if (/business & industrial|b2b|manufacturing|wholesale/.test(name)) return "service";
  return null;
}

/**
 * Extract the most prominent organisation name from NL entities
 */
// Generic words that NL API often wrongly picks as "organization" names
const GENERIC_BIZ_WORDS = new Set([
  "shop","store","website","site","page","home","cart","menu","blog","news",
  "team","contact","about","services","products","company","business","brand",
  "support","help","search","login","signup","account","checkout","order",
  "cookie","privacy","terms","policy","all","new","sale","free","best","top",
]);

function extractBusinessNameFromEntities(entities) {
  if (!entities || !entities.length) return null;
  const orgs = entities
    .filter((e) => {
      if (e.type !== "ORGANIZATION") return false;
      if ((e.salience || 0) <= 0.01) return false;
      const nameLower = (e.name || "").toLowerCase().trim();
      // Skip single generic words and very short names
      if (nameLower.length < 3) return false;
      if (GENERIC_BIZ_WORDS.has(nameLower)) return false;
      return true;
    })
    .sort((a, b) => (b.salience || 0) - (a.salience || 0));
  return orgs.length ? orgs[0].name : null;
}

/**
 * Extract location entities to further boost market detection
 */
function extractLocationsFromEntities(entities) {
  if (!entities || !entities.length) return [];
  return entities
    .filter((e) => e.type === "LOCATION" && (e.salience || 0) > 0.01)
    .sort((a, b) => (b.salience || 0) - (a.salience || 0))
    .map((e) => e.name);
}

async function safeBrowsingCheck(url) {
  if (!GOOGLE_API_KEY_MONEY) return false;
  try {
    // Use Web Risk API (v1) — replaces deprecated Safe Browsing API v4
    const res = await axios.get(
      `https://webrisk.googleapis.com/v1/uris:search`,
      {
        params: {
          key: GOOGLE_API_KEY_MONEY,
          uri: url,
          "threatTypes": ["MALWARE", "SOCIAL_ENGINEERING", "UNWANTED_SOFTWARE"],
        },
        timeout: 8000,
        validateStatus: () => true,
      }
    );
    // Web Risk returns { threat: { threatTypes: [...], expireTime, uri } } if threat found
    return !!(res.data && res.data.threat);
  } catch (e) {
    console.warn("[safe browsing] check failed:", e.message);
    return false;
  }
}

async function getIndexedPageCount(domain) {
  if (!GOOGLE_API_KEY_MONEY || !GOOGLE_CSE_ID) return null;
  try {
    const cleanDomain = domain.replace(/^https?:\/\//, "").replace(/\/$/, "");
    const res = await axios.get("https://www.googleapis.com/customsearch/v1", {
      params: {
        key: GOOGLE_API_KEY_MONEY,
        cx: GOOGLE_CSE_ID,
        q: `site:${cleanDomain}`,
        num: 1,
      },
      timeout: 8000,
    });
    const total = res.data?.searchInformation?.totalResults;
    return total ? parseInt(total, 10) : null;
  } catch (e) {
    const status = e.response?.status;
    const reason = e.response?.data?.error?.message || e.message;
    console.warn(`[custom search] indexed pages failed: ${status} – ${reason}`);
    return null;
  }
}

function getSiteHealthCTRPosition(issues) {
  const critical = (issues.non_indexable_pages || 0) +
    (issues.missing_titles || 0) +
    (issues.canonical_issues || 0);
  if (critical === 0) return 4;
  if (critical <= 2) return 7;
  if (critical <= 5) return 12;
  return 18;
}

function calculateMoneyImpact({ market, industry, issues, psiMobile, indexedPages, summaryState }) {
  const benchmark = BENCHMARKS[market]?.[industry] || BENCHMARKS[market]?.default || BENCHMARKS.US[industry] || BENCHMARKS.US.default;
  const valuePerVisitor = benchmark.valuePerVisitor;

  const effectiveIndexed = indexedPages || Math.max(summaryState.pages_crawled * 3, 5);
  const avgImpressionsPerPage = 200;
  const position = getSiteHealthCTRPosition(issues);
  const baseCtr = ctrAtPosition(position) / 100;
  const estimatedMonthlyClicks = Math.round(effectiveIndexed * avgImpressionsPerPage * baseCtr);

  const issueResults = [];

  // Missing title tags
  if (issues.missing_titles > 0) {
    const lostCtr = (ctrAtPosition(position) - ctrAtPosition(Math.min(position + 4, 20))) / 100;
    const lostClicks = estimatedMonthlyClicks * lostCtr * (issues.missing_titles / Math.max(summaryState.pages_crawled, 1));
    const loss = Math.round(lostClicks * valuePerVisitor);
    if (loss > 0) issueResults.push({ issueId: "missing_title", loss, min: Math.round(loss * 0.6), max: Math.round(loss * 1.2), fixDifficulty: "easy", fixTimeMinutes: 30 });
  }

  // Missing meta descriptions
  if (issues.missing_meta_descriptions > 0) {
    const lostCtr = (ctrAtPosition(position) - ctrAtPosition(Math.min(position + 2, 20))) / 100;
    const lostClicks = estimatedMonthlyClicks * lostCtr * (issues.missing_meta_descriptions / Math.max(summaryState.pages_crawled, 1));
    const loss = Math.round(lostClicks * valuePerVisitor);
    if (loss > 0) issueResults.push({ issueId: "missing_meta", loss, min: Math.round(loss * 0.6), max: Math.round(loss * 1.2), fixDifficulty: "easy", fixTimeMinutes: 45 });
  }

  // Missing H1
  if (issues.missing_h1s > 0) {
    const lostCtr = (ctrAtPosition(position) - ctrAtPosition(Math.min(position + 3, 20))) / 100;
    const lostClicks = estimatedMonthlyClicks * lostCtr * (issues.missing_h1s / Math.max(summaryState.pages_crawled, 1));
    const loss = Math.round(lostClicks * valuePerVisitor);
    if (loss > 0) issueResults.push({ issueId: "missing_h1", loss, min: Math.round(loss * 0.6), max: Math.round(loss * 1.2), fixDifficulty: "easy", fixTimeMinutes: 20 });
  }

  // Canonical issues
  if (issues.canonical_issues > 0) {
    const lostCtr = (ctrAtPosition(position) - ctrAtPosition(Math.min(position + 5, 20))) / 100;
    const lostClicks = estimatedMonthlyClicks * lostCtr * (issues.canonical_issues / Math.max(summaryState.pages_crawled, 1));
    const loss = Math.round(lostClicks * valuePerVisitor);
    if (loss > 0) issueResults.push({ issueId: "canonical_issues", loss, min: Math.round(loss * 0.6), max: Math.round(loss * 1.2), fixDifficulty: "medium", fixTimeMinutes: 60 });
  }

  // Non-indexable pages
  if (issues.non_indexable_pages > 0) {
    const lostClicks = estimatedMonthlyClicks * 0.15 * (issues.non_indexable_pages / Math.max(summaryState.pages_crawled, 1));
    const loss = Math.round(lostClicks * valuePerVisitor);
    if (loss > 0) issueResults.push({ issueId: "non_indexable", loss, min: Math.round(loss * 0.6), max: Math.round(loss * 1.2), fixDifficulty: "medium", fixTimeMinutes: 90 });
  }

  // Slow page speed (PSI)
  if (psiMobile != null) {
    let bounceIncrease = 0;
    if (psiMobile < 50) bounceIncrease = 0.45;
    else if (psiMobile < 90) bounceIncrease = 0.22;
    if (bounceIncrease > 0) {
      const loss = Math.round(estimatedMonthlyClicks * bounceIncrease * valuePerVisitor);
      if (loss > 0) issueResults.push({ issueId: "slow_speed", loss, min: Math.round(loss * 0.6), max: Math.round(loss * 1.2), fixDifficulty: "hard", fixTimeMinutes: 240 });
    }
  }

  // Thin content
  if (issues.thin_content_pages > 0) {
    const lostCtr = (ctrAtPosition(position) - ctrAtPosition(Math.min(position + 2, 20))) / 100;
    const lostClicks = estimatedMonthlyClicks * lostCtr * (issues.thin_content_pages / Math.max(summaryState.pages_crawled, 1));
    const loss = Math.round(lostClicks * valuePerVisitor);
    if (loss > 0) issueResults.push({ issueId: "thin_content", loss, min: Math.round(loss * 0.6), max: Math.round(loss * 1.2), fixDifficulty: "medium", fixTimeMinutes: 120 });
  }

  // Content gap losses — missing pages that cost traffic
  const hasPricingPage = Object.keys(issues.page_type_counts || {}).includes("pricing");
  const hasBlog = Object.keys(issues.page_type_counts || {}).includes("article") ||
                  Object.keys(issues.page_type_counts || {}).includes("blog");
  if (!hasPricingPage && (industry === "saas" || industry === "ecommerce" || industry === "service")) {
    const loss = Math.round(estimatedMonthlyClicks * 0.15 * valuePerVisitor);
    if (loss > 0) issueResults.push({ issueId: "no_pricing_page", loss, min: Math.round(loss * 0.6), max: Math.round(loss * 1.2), fixDifficulty: "medium", fixTimeMinutes: 180 });
  }
  if (!hasBlog) {
    const loss = Math.round(estimatedMonthlyClicks * 0.20 * valuePerVisitor);
    if (loss > 0) issueResults.push({ issueId: "no_content_pages", loss, min: Math.round(loss * 0.6), max: Math.round(loss * 1.2), fixDifficulty: "medium", fixTimeMinutes: 300 });
  }

  issueResults.sort((a, b) => b.loss - a.loss);

  let totalLossRaw = issueResults.reduce((sum, i) => sum + i.loss, 0);

  // Guarantee a minimum — even perfect sites have opportunity value from estimated traffic
  // Baseline = 25-50% of monthly traffic value (what they'd earn if they captured more search)
  const baselineMin = Math.round(estimatedMonthlyClicks * valuePerVisitor * 0.25);
  const baselineMax = Math.round(estimatedMonthlyClicks * valuePerVisitor * 0.50);
  if (totalLossRaw < baselineMin) totalLossRaw = baselineMin;

  const totalMin = Math.max(Math.round(totalLossRaw * 0.6), baselineMin);
  const totalMax = Math.max(Math.round(totalLossRaw * 1.2), baselineMax);

  return {
    valuePerVisitor,
    estimatedMonthlyClicks,
    issueResults,
    totalMonthlyLossMin: totalMin,
    totalMonthlyLossMax: totalMax,
  };
}

async function callOpenAI(model, systemPrompt, userPrompt) {
  if (!OPENAI_API_KEY) return null;
  try {
    const res = await axios.post(
      "https://api.openai.com/v1/chat/completions",
      {
        model,
        messages: [
          { role: "system", content: systemPrompt },
          { role: "user", content: userPrompt },
        ],
        temperature: 0.7,
        max_tokens: 800,
      },
      {
        headers: {
          Authorization: `Bearer ${OPENAI_API_KEY}`,
          "Content-Type": "application/json",
        },
        timeout: 30000,
      }
    );
    return res.data.choices[0].message.content;
  } catch (e) {
    console.warn("[openai] call failed:", e.message);
    return null;
  }
}

async function generateExecutiveSummary({ market, symbol, businessName, issueCount, totalMin, totalMax, psiScore, indexedPages, industry, nlSentiment, nlEntities, nlCategories }) {
  const systemPrompt = `You are a trusted business advisor speaking to a business owner in the ${market} market. Write a business health report executive summary. Plain language. Money focused. Zero jargon. Under 100 words. Use ${symbol} for all figures. ${market === "UK" ? "UK tone: measured, professional." : "US tone: direct, confident, action-oriented."}`;

  // Build NL context block if available
  const nlContext = [];
  if (nlSentiment != null) nlContext.push(`Content sentiment: ${nlSentiment > 0.2 ? "positive" : nlSentiment < -0.2 ? "negative" : "neutral"} (score: ${nlSentiment.toFixed(2)})`);
  if (nlEntities?.length)  nlContext.push(`Key topics on site: ${nlEntities.slice(0, 6).join(", ")}`);
  if (nlCategories?.length) nlContext.push(`Google content categories: ${nlCategories.slice(0, 3).join(", ")}`);

  const userPrompt = `Business: ${businessName || "this website"} — ${industry} business
Market: ${market} | Currency: ${symbol}
Issues found: ${issueCount}
Estimated monthly loss: ${symbol}${totalMin} – ${symbol}${totalMax}
PageSpeed score: ${psiScore ?? "unknown"}/100
Google indexed pages: ${indexedPages ?? "unknown"}${nlContext.length ? "\n" + nlContext.join("\n") : ""}
Write the summary now. Plain text only, no markdown.`;
  return await callOpenAI("gpt-4o", systemPrompt, userPrompt);
}

async function generateIssueNarrative({ issueId, market, symbol, industry, loss, min, max, valuePerVisitor, nlEntities, nlSentiment }) {
  const issueDescriptions = {
    missing_title: "Missing or weak title tags on key pages",
    missing_meta: "Missing meta descriptions across pages",
    missing_h1: "Missing H1 headings on key pages",
    canonical_issues: "Canonical tag errors causing duplicate content",
    non_indexable: "Pages blocked from Google's index",
    slow_speed: "Slow page load speed",
    thin_content: "Thin content pages with low word count",
  };

  const systemPrompt = `You are a trusted business advisor speaking to a ${market} business owner with ZERO technical knowledge. They only care about money.
Rules:
- NEVER use: SEO, canonical, meta, H1, crawl, index, algorithm, SERP, backlink, schema, hreflang
- Always use ${symbol} for money
- Be like a trusted accountant — not a salesperson
- ${market === "UK" ? "UK tone: measured, honest, professional. Use: high street, till, fortnight" : "US tone: direct, confident, action-oriented. Use: storefront, checkout"}
Return ONLY valid JSON:
{"headline":"string (max 10 words)","explanation":"string (relatable metaphor, 2 sentences)","moneyImpact":"string (exact figure in ${symbol})","fix":["step 1","step 2","step 3"],"urgency":"critical|high|medium"}`;

  const userPrompt = `Market: ${market} | Currency: ${symbol}
Business type: ${industry}
Issue: ${issueDescriptions[issueId] || issueId}
Monthly loss: ${symbol}${min} – ${symbol}${max}
Value per visitor: ${symbol}${valuePerVisitor}${nlEntities?.length ? `\nKey site topics: ${nlEntities.slice(0, 4).join(", ")}` : ""}${nlSentiment != null ? `\nContent sentiment: ${nlSentiment > 0.2 ? "positive" : nlSentiment < -0.2 ? "negative" : "neutral"}` : ""}`;

  const raw = await callOpenAI("gpt-4o-mini", systemPrompt, userPrompt);
  if (!raw) return null;
  try {
    const jsonMatch = raw.match(/\{[\s\S]*\}/);
    return jsonMatch ? JSON.parse(jsonMatch[0]) : null;
  } catch {
    return null;
  }
}

async function runMoneyEngine(siteId, snapshotId, seedUrl, summaryState) {
  console.log(`[money engine] starting for snapshot=${snapshotId}`);

  // 1a. Google Natural Language API — classify industry + extract entities from homepage
  let nlResult = null;
  let nlIndustry = null;
  let nlBusinessName = null;
  let nlLocations = [];
  let nlSentiment = null;
  let nlTopEntities = []; // top entity names for OpenAI context
  let nlTopCategories = []; // raw category names for OpenAI context
  if (summaryState.homepage_text_snippet) {
    nlResult = await callNaturalLanguageAPI(summaryState.homepage_text_snippet);
    if (nlResult) {
      nlIndustry = mapNLCategoriesToIndustry(nlResult.categories);
      nlBusinessName = extractBusinessNameFromEntities(nlResult.entities);
      nlLocations = extractLocationsFromEntities(nlResult.entities);
      nlSentiment = nlResult.documentSentiment?.score ?? null;
      // Top entities by salience (excluding generic words already filtered in extractBusinessNameFromEntities)
      nlTopEntities = (nlResult.entities || [])
        .filter(e => (e.salience || 0) > 0.01 && e.name && e.name.length > 2)
        .sort((a, b) => (b.salience || 0) - (a.salience || 0))
        .slice(0, 10)
        .map(e => e.name);
      nlTopCategories = (nlResult.categories || []).map(c => c.name).slice(0, 5);
      console.log(`[nl api] industry=${nlIndustry} bizName=${nlBusinessName} locations=${nlLocations.join(",")} sentiment=${nlSentiment?.toFixed(2)} entities=${nlTopEntities.slice(0,5).join(",")}`);
    }
  }

  // 1b. Market detection — content signals + TLD fallback
  // Boost location_signals with NL-extracted locations
  if (nlLocations.length && summaryState.location_signals) {
    const ukPlaces = /london|england|scotland|wales|britain|manchester|birmingham|edinburgh|glasgow|bristol|liverpool|leeds|sheffield/i;
    const usPlaces = /new york|los angeles|chicago|houston|phoenix|dallas|san francisco|seattle|boston/i;
    const auPlaces = /sydney|melbourne|brisbane|perth|adelaide/i;
    for (const loc of nlLocations) {
      if (ukPlaces.test(loc)) summaryState.location_signals.uk_country += 5;
      else if (usPlaces.test(loc)) summaryState.location_signals.us_country += 5;
      else if (auPlaces.test(loc)) summaryState.location_signals.au_country += 5;
    }
  }

  // 1c. Final market + industry
  const { market, currency, symbol } = detectMarketFromSignals(summaryState.location_signals, seedUrl);
  const industry = nlIndustry || classifyIndustry(summaryState.page_type_counts, summaryState.site_type, seedUrl);
  console.log(`[money engine] market=${market} industry=${industry} (nl=${nlIndustry}) signals=${JSON.stringify(summaryState.location_signals)}`);

  // 2. Safe browsing check
  const isThreat = await safeBrowsingCheck(seedUrl);
  if (isThreat) console.warn(`[money engine] SAFE BROWSING THREAT detected for ${seedUrl}`);

  // 3. Indexed page count via Custom Search
  const indexedPages = await getIndexedPageCount(seedUrl);
  console.log(`[money engine] indexedPages=${indexedPages}`);

  // 4. Get PSI score from DB for this snapshot
  let psiMobile = null;
  try {
    const { data: psiRows } = await supabase
      .from("scc_page_snapshot_metrics")
      .select("performance_score_mobile")
      .eq("snapshot_id", snapshotId)
      .not("performance_score_mobile", "is", null)
      .order("performance_score_mobile", { ascending: true })
      .limit(1);
    if (psiRows && psiRows.length > 0) psiMobile = psiRows[0].performance_score_mobile;
  } catch (e) {
    console.warn("[money engine] psi fetch failed:", e.message);
  }

  // 5. Calculate money impact
  const money = calculateMoneyImpact({
    market,
    industry,
    issues: summaryState.issues,
    psiMobile,
    indexedPages,
    summaryState,
  });

  // 6. Get business name — prefer NL-extracted name, then DB, then URL
  let businessName = nlBusinessName || null;
  try {
    const { data: siteRow } = await supabase.from("scc_sites").select("name, url").eq("id", siteId).single();
    if (!businessName) businessName = siteRow?.name || siteRow?.url || seedUrl;
  } catch {}

  // 7. Generate executive summary — include NL context so OpenAI has richer data
  const execSummary = await generateExecutiveSummary({
    market, symbol, businessName,
    issueCount: money.issueResults.length,
    totalMin: money.totalMonthlyLossMin,
    totalMax: money.totalMonthlyLossMax,
    psiScore: psiMobile,
    indexedPages,
    industry,
    nlSentiment,
    nlEntities: nlTopEntities,
    nlCategories: nlTopCategories,
  });

  // 8. Update snapshot with money data
  const confidenceScore = indexedPages ? 55 : 40;
  await supabase.from("scc_snapshots").update({
    market,
    currency,
    currency_symbol: symbol,
    business_type: summaryState.site_type,
    business_name: businessName,
    industry,
    value_per_visitor: money.valuePerVisitor,
    estimated_monthly_traffic: money.estimatedMonthlyClicks,
    total_monthly_loss_min: money.totalMonthlyLossMin,
    total_monthly_loss_max: money.totalMonthlyLossMax,
    safe_browsing_threat: isThreat,
    indexed_page_count: indexedPages,
    executive_summary: execSummary,
    confidence_score: confidenceScore,
  }).eq("id", snapshotId);

  // Also merge money data into notes JSON so frontend can read without schema cache issues
  const { data: existingSnap } = await supabase.from("scc_snapshots").select("notes").eq("id", snapshotId).single();
  const existingNotes = (() => { try { return JSON.parse(existingSnap?.notes || "{}"); } catch { return {}; } })();
  const updatedNotes = {
    ...existingNotes,
    money: {
      total_monthly_loss_min: money.totalMonthlyLossMin,
      total_monthly_loss_max: money.totalMonthlyLossMax,
      currency_symbol: symbol,
      market,
      industry,
      value_per_visitor: money.valuePerVisitor,
      estimated_monthly_traffic: money.estimatedMonthlyClicks,
      confidence_score: confidenceScore,
      executive_summary: execSummary,
      safe_browsing_threat: isThreat,
      indexed_page_count: indexedPages,
      business_name: businessName,
      // NL API enrichment
      nl_industry_raw: nlResult?.categories?.[0]?.name || null,
      nl_sentiment_score: nlResult?.documentSentiment?.score ?? null,
      nl_sentiment_magnitude: nlResult?.documentSentiment?.magnitude ?? null,
      nl_top_entities: nlResult?.entities
        ? nlResult.entities
            .filter((e) => (e.salience || 0) > 0.02)
            .slice(0, 8)
            .map((e) => ({ name: e.name, type: e.type, salience: e.salience }))
        : [],
      nl_locations: nlLocations.slice(0, 5),
    },
  };
  await supabase.from("scc_snapshots").update({ notes: JSON.stringify(updatedNotes) }).eq("id", snapshotId);

  // 9. Generate AI narratives for top issues and update actions
  // Map money engine issue IDs → scc_actions action_type values
  const ISSUE_TO_ACTION_TYPE = {
    missing_title:    "site_missing_titles",
    missing_meta:     "site_missing_meta_descriptions",
    missing_h1:       "site_missing_h1s",
    non_indexable:    "site_non_indexable_pages",
    thin_content:     "site_thin_content",
    slow_speed:       "site_slow_speed",
    canonical_issues: "site_canonical_issues",
  };

  const top5 = money.issueResults.slice(0, 5);
  await Promise.all(
    top5.map(async (issue) => {
      const narrative = await generateIssueNarrative({ ...issue, market, symbol, industry, valuePerVisitor: money.valuePerVisitor, nlEntities: nlTopEntities, nlSentiment });
      if (!narrative) return;

      const actionType = ISSUE_TO_ACTION_TYPE[issue.issueId];
      const updatePayload = {
        money_loss_min: issue.min,
        money_loss_max: issue.max,
        fix_difficulty: issue.fixDifficulty,
        fix_time_minutes: issue.fixTimeMinutes,
        urgency: narrative.urgency,
        title: narrative.headline,
        why_it_matters: narrative.explanation,   // was wrongly 'summary'
        expected_impact_range: narrative.moneyImpact,
        steps: narrative.fix,
      };

      if (actionType) {
        // Update existing site-wide action matched by action_type (exact match, not fragile ilike)
        const { error } = await supabase.from("scc_actions")
          .update(updatePayload)
          .eq("snapshot_id", snapshotId)
          .eq("action_type", actionType);
        if (error) console.warn(`[narrative] update failed for ${actionType}:`, error.message);
        else console.log(`[narrative] updated action_type=${actionType} headline="${narrative.headline}"`);
      } else {
        // No site-wide action exists for this issue — insert a new one
        const { error } = await supabase.from("scc_actions").insert({
          snapshot_id: snapshotId,
          action_type: `site_${issue.issueId}`,
          severity: issue.min > 100 ? "high" : "medium",
          priority: 50,
          status: "pending",
          ...updatePayload,
        });
        if (error) console.warn(`[narrative] insert failed for ${issue.issueId}:`, error.message);
        else console.log(`[narrative] inserted new action for ${issue.issueId} headline="${narrative.headline}"`);
      }
    })
  );

  console.log(`[money engine] done. loss=${symbol}${money.totalMonthlyLossMin}-${symbol}${money.totalMonthlyLossMax} confidence=${confidenceScore}%`);
}

async function rescueStaleJobs() {
  try {
    await supabase.rpc("scc_rescue_stale_jobs", {
      p_minutes: RESCUE_STALE_AFTER_MIN,
    });
  } catch (err) {
    console.error("[rescue stale jobs]", err.message);
  }
}

async function claimNextJob() {
  const { data, error } = await supabase.rpc("scc_claim_next_job", {
    p_worker_id: WORKER_ID,
  });

  if (error) throw error;
  if (!data) return null;

  if (Array.isArray(data)) return data[0] || null;
  if (typeof data === "object" && data.job) return data.job;
  return data;
}

async function main() {
  console.log(`[worker boot] ${WORKER_ID}`);

  while (true) {
    try {
      await rescueStaleJobs();

      const job = await claimNextJob();
      if (!job) {
        await sleep(POLL_MS);
        continue;
      }

      await runCrawlJob(job);
    } catch (err) {
      console.error("[worker loop error]", err);
      await sleep(POLL_MS);
    }
  }
}

main().catch((err) => {
  console.error("[fatal worker error]", err);
  process.exit(1);
});
