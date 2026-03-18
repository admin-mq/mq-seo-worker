import { createClient } from "@supabase/supabase-js";
import axios from "axios";
import * as cheerio from "cheerio";

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
const USER_AGENT =
  process.env.USER_AGENT ||
  "Mozilla/5.0 (compatible; MarketersQuestSEO/2.4; +https://marketersquest.com)";
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

  if (path === "/" || path === "") {
    return "homepage";
  }

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
  if (/contact|support|help|customer-service/.test(path)) {
    addScore(scores, "contact", 90);
  }
  if (/about|company|our-story|who-we-are|team|leadership/.test(path)) {
    addScore(scores, "about", 80);
  }
  if (/pricing|plans/.test(path)) {
    addScore(scores, "pricing", 95);
  }
  if (/demo|book-demo|get-started|start-now|free-trial|trial|contact-sales/.test(path)) {
    addScore(scores, "conversion", 95);
  }
  if (/location|locations|city|area|near-me/.test(path)) {
    addScore(scores, "location", 75);
  }
  if (/case-study|case-studies|success-story|success-stories/.test(path)) {
    addScore(scores, "case_study", 90);
  }
  if (/testimonial|testimonials|review|reviews/.test(path)) {
    addScore(scores, "proof", 80);
  }
  if (/feature|features|capabilities|platform|technology/.test(path)) {
    addScore(scores, "feature", 70);
  }
  if (/service|services|solution|solutions/.test(path)) {
    addScore(scores, "service", 72);
  }
  if (/product|products|sku|item|\/p\/|\/pdp\/|\/product\//.test(path)) {
    addScore(scores, "product", 85);
  }
  if (/category|categories/.test(path)) {
    addScore(scores, "category", 90);
  }
  if (/collection|collections|shop|store|catalog|browse/.test(path)) {
    addScore(scores, "category", 68);
  }
  if (isArchiveLikeUrl(normalizedUrl)) {
    addScore(scores, "archive", 100);
  }
  if (isArticleLikeUrl(normalizedUrl)) {
    addScore(scores, "article", 88);
  }

  if (segs.length >= 2 && segs.some(isLikelyDateSegment)) {
    addScore(scores, "article", 25);
    addScore(scores, "archive", -8);
  }

  if (schema.some((s) => ["article", "blogposting", "newsarticle", "medicalwebpage", "howto"].includes(s))) {
    addScore(scores, "article", 30);
  }
  if (schema.some((s) => ["product"].includes(s))) {
    addScore(scores, "product", 30);
  }
  if (schema.some((s) => ["faqpage", "softwareapplication", "service"].includes(s))) {
    addScore(scores, "service", 14);
    addScore(scores, "feature", 10);
  }

  if (/price|pricing|plans|cost/.test(combined)) {
    addScore(scores, "pricing", 26);
  }
  if (/book demo|request demo|get started|free trial|start free|contact sales|enquire now/.test(combined)) {
    addScore(scores, "conversion", 28);
  }
  if (/service|services|solution|solutions/.test(combined)) {
    addScore(scores, "service", 18);
  }
  if (/case study|success story/.test(combined)) {
    addScore(scores, "case_study", 24);
  }
  if (/testimonial|review|client story/.test(combined)) {
    addScore(scores, "proof", 18);
  }
  if (/about us|our company|our team/.test(combined)) {
    addScore(scores, "about", 22);
  }
  if (/contact us|support/.test(combined)) {
    addScore(scores, "contact", 22);
  }
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
  if (/category archives|tag archives|author archives/.test(body)) {
    addScore(scores, "archive", 25);
  }

  if (pathDepth(normalizedUrl) >= 2 && slug.split("-").length >= 3) {
    addScore(scores, "article", 10);
  }
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
    if ((scores[rule.type] || 0) >= rule.min) {
      return rule.type;
    }
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
      const urlObj = safeUrl(href, baseUrl);
      if (!urlObj) return;

      const normalized = normalizeUrl(urlObj.toString());
      if (!normalized) return;
      if (seen.has(normalized)) return;

      seen.add(normalized);
      links.push({
        url: normalized,
        anchorText,
        source: "nav",
      });
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

    if (!href) return;
    if (href.startsWith("#")) return;
    if (/^(mailto:|tel:|javascript:)/i.test(href)) return;

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

    results.push({
      url: normalized,
      anchorText,
      inNav,
    });
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

      if (selectedArticles < targets.minArticles) {
        adjusted += 24;
      }

      if (parentPageType === "archive" || parentPageType === "category") {
        adjusted += 18;
      }

      if (/read more|continue reading|article|post|story|guide/.test((anchorText || "").toLowerCase())) {
        adjusted += 10;
      }
    }

    if (pageType === "archive") {
      adjusted -= 10;

      if (selectedArchives >= targets.maxArchives) {
        adjusted -= 34;
      }

      if (familySelected >= 1) {
        adjusted -= 12;
      }

      if (familyEnqueued >= 2) {
        adjusted -= 8;
      }
    }

    if (pageType === "category") {
      adjusted -= 4;

      if (selectedCategories >= targets.maxCategories) {
        adjusted -= 24;
      }

      if (familySelected >= 1) {
        adjusted -= 10;
      }
    }

    if (familySelected >= targets.maxSameFamily) {
      adjusted -= 20;
    }

    if (pageType === "general" && parentPageType === "archive") {
      adjusted -= 8;
    }

    if (
      selectedArticles < targets.minArticles &&
      (pageType === "archive" || pageType === "category")
    ) {
      adjusted -= 12;
    }
  }

  if (siteType === "service") {
    if (pageType === "article" || pageType === "archive") adjusted -= 8;
  }

  if (siteType === "ecommerce") {
    if (pageType === "article" || pageType === "archive") adjusted -= 10;
  }

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
  const pageType = classifyPageTypeFromSignals({
    url: candidateUrl,
    anchorText,
  });

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

  return {
    score,
    pageType,
    familyKey,
  };
}

async function fetchHtml(url) {
  const started = Date.now();

  const response = await axios.get(url, {
    timeout: REQUEST_TIMEOUT_MS,
    maxRedirects: MAX_REDIRECTS,
    validateStatus: () => true,
    headers: {
      "User-Agent": USER_AGENT,
      Accept: "text/html,application/xhtml+xml",
    },
  });

  const loadMs = Date.now() - started;
  const contentType = (response.headers["content-type"] || "").toLowerCase();

  return {
    status: response.status,
    html: typeof response.data === "string" ? response.data : "",
    contentType,
    finalUrl: normalizeUrl(response.request?.res?.responseUrl || url) || normalizeUrl(url),
    loadMs,
  };
}

function extractSeoData(html, url, status, contentType, loadMs, depth, seedUrl) {
  const $ = cheerio.load(html || "");

  const title = cleanText($("title").first().text() || "");
  const metaDescription = cleanText(
    $('meta[name="description"]').attr("content") || ""
  );

  const canonicalHref = $('link[rel="canonical"]').attr("href");
  const canonicalUrl = canonicalHref
    ? normalizeUrl(safeUrl(canonicalHref, url)?.toString())
    : null;

  const h1Count = $("h1").length;
  const h1Text = cleanText($("h1").first().text() || "");
  const bodyText = cleanText($("body").text() || "");
  const wordCount = countWords(bodyText);

  const robotsMeta = (
    $('meta[name="robots"]').attr("content") ||
    $('meta[name="googlebot"]').attr("content") ||
    ""
  ).toLowerCase();

  const noindex = robotsMeta.includes("noindex");
  const indexable =
    status >= 200 &&
    status < 300 &&
    contentType.includes("text/html") &&
    !noindex;

  const schemaTypes = detectSchemaTypes($);

  const pageType = classifyPageTypeFromSignals({
    url,
    title,
    h1Text,
    bodyText,
    schemaTypes,
  });

  const internalLinks = extractInternalLinks($, url, seedUrl);

  return {
    $,
    title,
    metaDescription,
    canonicalUrl,
    h1Count,
    h1Text: h1Text || null,
    bodyText,
    wordCount,
    robotsMeta: robotsMeta || null,
    noindex,
    indexable,
    schemaTypes,
    pageType,
    statusCode: status,
    contentType,
    loadMs,
    internalLinks,
    internalLinksCount: internalLinks.length,
    internalLinkDepth: depth,
  };
}

function evaluateCanonicalOk(finalUrl, canonicalUrl) {
  if (!canonicalUrl) return false;
  const normalizedFinal = normalizeUrl(finalUrl);
  const normalizedCanonical = normalizeUrl(canonicalUrl);
  return normalizedFinal && normalizedCanonical
    ? normalizedFinal === normalizedCanonical
    : false;
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

  if (["policy", "contact", "about"].includes(pageType)) {
    score -= 18;
  }

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

  if (["archive", "policy", "contact"].includes(pageType)) {
    score -= 10;
  }

  if (pageType === "about") {
    score -= 4;
  }

  if (intentWeight <= 0.1) {
    score = Math.min(score, 24);
  }

  return clamp(Math.round(score), 0, 100);
}

function computePriorityBucket(opportunityScore, revenueScore, pageType) {
  if (["policy", "contact"].includes(pageType) && opportunityScore < 30) {
    return "Tier 4";
  }

  if (opportunityScore >= 78) return "Tier 1";
  if (opportunityScore >= 58) return "Tier 2";
  if (opportunityScore >= 35) return "Tier 3";
  return "Tier 4";
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
}) {
  const actions = [];

  const pushAction = ({
    actionType,
    summary,
    priority,
    severity,
    titleText,
    whyItMatters,
    technicalReason,
    expectedImpactRange,
    steps,
    score = 50,
  }) => {
    actions.push({
      action_type: actionType,
      summary,
      priority,
      status: "pending",
      title: titleText,
      why_it_matters: whyItMatters,
      technical_reason: technicalReason,
      expected_impact_range: expectedImpactRange,
      steps,
      severity,
      _sortScore: score,
    });
  };

  if (statusCode >= 400) {
    pushAction({
      actionType: "fix_status_code",
      summary: `This page returns HTTP ${statusCode} and may be blocking SEO visibility.`,
      priority: "critical",
      severity: "high",
      titleText: `Fix HTTP ${statusCode} page issue`,
      whyItMatters: "Broken pages waste crawl budget and prevent traffic from landing on usable content.",
      technicalReason: `The crawler received HTTP ${statusCode} for this page.`,
      expectedImpactRange: "Medium-High",
      steps: [
        "Confirm whether this URL should remain live.",
        "Restore the page if it should exist.",
        "Otherwise redirect it to the closest relevant working page.",
        "Update internal links that still point here."
      ],
      score: 98,
    });
  }

  if (!indexable && statusCode >= 200 && statusCode < 300) {
    pushAction({
      actionType: "review_indexability",
      summary: "This page appears live but may not be indexable.",
      priority: ["pricing", "conversion", "service", "product", "homepage"].includes(pageType) ? "critical" : "high",
      severity: "high",
      titleText: "Review indexability settings",
      whyItMatters: "A page that cannot be indexed will struggle to earn organic visibility.",
      technicalReason: "The page is live but crawl signals suggest it should not be indexed.",
      expectedImpactRange: "High",
      steps: [
        "Check robots meta directives on the page.",
        "Confirm whether noindex is intentional.",
        "Remove noindex from pages that should rank."
      ],
      score: 94,
    });
  }

  if (!hasTitle || !title || title.length < 20) {
    pushAction({
      actionType: "improve_title",
      summary: "The page title is missing or too weak.",
      priority: ["pricing", "conversion", "service", "product", "homepage"].includes(pageType) ? "high" : "medium",
      severity: "high",
      titleText: "Improve page title",
      whyItMatters: "Title tags are one of the strongest on-page signals for ranking and click-through.",
      technicalReason: "The page is missing a descriptive title or the title is too short to communicate intent well.",
      expectedImpactRange: "Medium",
      steps: [
        "Write a unique title for this page.",
        "Place the core keyword near the beginning.",
        "Make the title specific to the page intent."
      ],
      score: 84,
    });
  }

  if (!hasMeta || !metaDescription || metaDescription.length < 110) {
    pushAction({
      actionType: "improve_meta_description",
      summary: "The meta description is missing or too short.",
      priority: ["pricing", "conversion", "service", "product", "homepage"].includes(pageType) ? "high" : "medium",
      severity: "medium",
      titleText: "Improve meta description",
      whyItMatters: "A stronger meta description can improve click-through rate from search results.",
      technicalReason: "The page lacks a clear, sufficiently descriptive meta description.",
      expectedImpactRange: "Low-Medium",
      steps: [
        "Write a concise description of 140 to 160 characters.",
        "Reflect the user intent of this page.",
        "Include a clear value proposition."
      ],
      score: 74,
    });
  }

  if (!hasH1) {
    pushAction({
      actionType: "add_h1",
      summary: "This page has no H1 heading.",
      priority: "high",
      severity: "high",
      titleText: "Add a clear H1",
      whyItMatters: "A clear H1 helps both users and search engines understand page focus.",
      technicalReason: "No H1 element was found on the page.",
      expectedImpactRange: "Medium",
      steps: [
        "Add one primary H1 to the page.",
        "Align it with the page’s core search intent.",
        "Avoid duplicating multiple competing H1s."
      ],
      score: 82,
    });
  } else if (h1Count > 1) {
    pushAction({
      actionType: "reduce_multiple_h1s",
      summary: "This page has multiple H1 tags.",
      priority: "medium",
      severity: "medium",
      titleText: "Reduce multiple H1 tags",
      whyItMatters: "Multiple H1s can weaken page hierarchy and reduce topical clarity.",
      technicalReason: `${h1Count} H1 tags were found.`,
      expectedImpactRange: "Low-Medium",
      steps: [
        "Keep one primary H1 on the page.",
        "Convert secondary top headings to H2 or H3."
      ],
      score: 58,
    });
  }

  const thinThreshold = getThinContentThreshold(pageType);
  if (wordCount < thinThreshold && ["service", "pricing", "product", "category", "article", "general", "homepage", "case_study", "feature"].includes(pageType)) {
    pushAction({
      actionType: "expand_content",
      summary: "The page content looks thin for its intent.",
      priority: ["pricing", "service", "product", "homepage"].includes(pageType) ? "high" : "medium",
      severity: "medium",
      titleText: "Expand thin content",
      whyItMatters: "Thin pages often struggle to rank and convert because they lack depth and clarity.",
      technicalReason: `The page appears to contain only about ${wordCount} words, which is below the expected depth for this page type.`,
      expectedImpactRange: "Medium",
      steps: [
        "Add deeper information related to the page topic.",
        "Address user questions more directly.",
        "Improve topical depth using stronger headings and sections."
      ],
      score: 76,
    });
  }

  if (!canonicalOk) {
    pushAction({
      actionType: "review_canonical",
      summary: "Canonical setup may be missing or not aligned to this page.",
      priority: ["pricing", "conversion", "service", "product", "category"].includes(pageType) ? "medium" : "low",
      severity: "medium",
      titleText: "Review canonical tag",
      whyItMatters: "Canonical tags help search engines consolidate duplicate and near-duplicate page signals.",
      technicalReason: "The page canonical is missing or does not point cleanly to the crawled final URL.",
      expectedImpactRange: "Low-Medium",
      steps: [
        "Check whether the page should self-canonicalize.",
        "Fix the canonical if it points to the wrong URL.",
        "Ensure duplicate variants consolidate to the preferred page."
      ],
      score: 60,
    });
  }

  if (pageOpportunityScore >= 80 && ["pricing", "conversion", "service", "product", "homepage"].includes(pageType)) {
    pushAction({
      actionType: "prioritize_high_value_page",
      summary: "This is a high-opportunity page and should be prioritized in your SEO roadmap.",
      priority: "critical",
      severity: "high",
      titleText: "Prioritize this page",
      whyItMatters: "Improvements on high-value pages can drive outsized traffic, leads, or revenue impact.",
      technicalReason: "The page combines strong business importance with meaningful optimization gaps.",
      expectedImpactRange: "High",
      steps: [
        "Address all technical and content issues on this page first.",
        "Improve internal linking to this page.",
        "Use this page as an early SEO win candidate."
      ],
      score: 92,
    });
  }

  return actions
    .sort((a, b) => b._sortScore - a._sortScore)
    .map(({ _sortScore, ...rest }) => rest);
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

async function heartbeat(jobId) {
  try {
    await supabase.rpc("scc_job_heartbeat", {
      p_job_id: jobId,
      p_worker_id: WORKER_ID,
    });
  } catch (err) {
    console.error(`[heartbeat] job=${jobId}`, err.message);
  }
}

async function completeJob(jobId, success, errorText = null) {
  const { error } = await supabase.rpc("scc_complete_crawl_job", {
    p_job_id: jobId,
    p_success: success,
    p_error: errorText,
  });

  if (error) throw error;
}

async function updateJobProgress(jobId, pagesDone, errorsCount) {
  const { error } = await supabase
    .from("scc_crawl_jobs")
    .update({
      pages_done: pagesDone,
      errors_count: errorsCount,
      last_heartbeat_at: nowIso(),
    })
    .eq("id", jobId);

  if (error) {
    console.error(`[progress update] job=${jobId}`, error.message);
  }
}

async function getOrCreatePage({ siteId, url, pageType }) {
  const now = nowIso();

  const { data: existing, error: existingError } = await supabase
    .from("scc_pages")
    .select("id")
    .eq("site_id", siteId)
    .eq("url", url)
    .maybeSingle();

  if (existingError) throw existingError;

  if (existing?.id) {
    const { error: updateError } = await supabase
      .from("scc_pages")
      .update({
        page_type: pageType,
        last_seen_at: now,
      })
      .eq("id", existing.id);

    if (updateError) throw updateError;
    return existing.id;
  }

  const { data: inserted, error: insertError } = await supabase
    .from("scc_pages")
    .insert({
      site_id: siteId,
      url,
      page_type: pageType,
      first_seen_at: now,
      last_seen_at: now,
    })
    .select("id")
    .single();

  if (insertError) throw insertError;
  return inserted.id;
}

async function upsertPageSnapshotCrawl({
  snapshotId,
  pageId,
  crawlRow,
}) {
  const { error } = await supabase
    .from("scc_page_snapshot_crawl")
    .upsert(
      {
        snapshot_id: snapshotId,
        page_id: pageId,
        ...crawlRow,
      },
      { onConflict: "snapshot_id,page_id" }
    );

  if (error) throw error;
}

async function upsertPageSnapshotMetrics({
  snapshotId,
  pageId,
  metricsRow,
}) {
  const { error } = await supabase
    .from("scc_page_snapshot_metrics")
    .upsert(
      {
        snapshot_id: snapshotId,
        page_id: pageId,
        ...metricsRow,
      },
      { onConflict: "snapshot_id,page_id" }
    );

  if (error) throw error;
}

async function replaceActions({
  snapshotId,
  pageId,
  actions,
}) {
  const { error: deleteError } = await supabase
    .from("scc_actions")
    .delete()
    .eq("snapshot_id", snapshotId)
    .eq("page_id", pageId);

  if (deleteError) throw deleteError;

  if (!actions.length) return;

  const rows = actions.map((action) => ({
    snapshot_id: snapshotId,
    page_id: pageId,
    query_id: null,
    action_type: action.action_type,
    summary: action.summary,
    priority: action.priority,
    status: action.status || "pending",
    title: action.title,
    why_it_matters: action.why_it_matters,
    technical_reason: action.technical_reason,
    expected_impact_range: action.expected_impact_range,
    steps: action.steps,
    severity: action.severity,
  }));

  const { error: insertError } = await supabase
    .from("scc_actions")
    .insert(rows);

  if (insertError) throw insertError;
}

async function processSinglePage({
  siteId,
  snapshotId,
  url,
  depth,
  seedUrl,
}) {
  let fetched;
  let fetchError = null;

  try {
    fetched = await fetchHtml(url);
  } catch (err) {
    fetchError = err.message || "Unknown fetch error";

    const pageType = classifyPageTypeFromSignals({ url });
    const pageId = await getOrCreatePage({
      siteId,
      url,
      pageType,
    });

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
        structural_score: 0,
        visibility_score: 0,
        revenue_score: Math.round(getPageIntentWeight(pageType) * 100),
        paid_risk_score: Math.round(getPageIntentWeight(pageType) * 40),
        page_opportunity_score: ["pricing", "conversion", "service", "product", "homepage"].includes(pageType) ? 52 : 35,
        priority_bucket: ["pricing", "conversion", "service", "product", "homepage"].includes(pageType) ? "Tier 3" : "Tier 4",
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
      pageOpportunityScore: 35,
    });

    await replaceActions({
      snapshotId,
      pageId,
      actions,
    });

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
    const pageId = await getOrCreatePage({
      siteId,
      url: effectiveUrl,
      pageType,
    });

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
        structural_score: 0,
        visibility_score: 0,
        revenue_score: Math.round(getPageIntentWeight(pageType) * 100),
        paid_risk_score: 0,
        page_opportunity_score: 10,
        priority_bucket: "Tier 4",
      },
    });

    await replaceActions({
      snapshotId,
      pageId,
      actions: [],
    });

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

  const pageId = await getOrCreatePage({
    siteId,
    url: effectiveUrl,
    pageType,
  });

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
  });

  await replaceActions({
    snapshotId,
    pageId,
    actions,
  });

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

  if (error) {
    console.error(`[snapshot running update] snapshot=${snapshotId}`, error.message);
  }
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

  if (error) {
    console.error(`[snapshot completed update] snapshot=${snapshotId}`, error.message);
  }
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

  if (error) {
    console.error(`[snapshot failed update] snapshot=${snapshotId}`, error.message);
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

  if (!seedUrl) {
    throw new Error("Invalid seed_url on crawl job");
  }

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

  const heartbeatTimer = setInterval(() => {
    heartbeat(jobId);
  }, HEARTBEAT_MS);

  try {
    await markSnapshotRunning(snapshotId);
    await heartbeat(jobId);

    const homepageResult = await processSinglePage({
      siteId,
      snapshotId,
      url: seedUrl,
      depth: 0,
      seedUrl,
    });

    seen.add(homepageResult.url);
    pagesDone += 1;
    if (homepageResult.fetchError) errorsCount += 1;

    registerSelectedPage(queueState, homepageResult.pageType, getUrlFamily(homepageResult.url, homepageResult.pageType));

    await updateJobProgress(jobId, pagesDone, errorsCount);

    if (pagesDone >= maxPages) {
      clearInterval(heartbeatTimer);
      await markSnapshotFinished(snapshotId);
      await completeJob(jobId, true, null);
      console.log(`[job done] id=${jobId} pages=${pagesDone}`);
      return;
    }

    let homepageLinks = homepageResult.links || [];
    try {
      const homeFetch = await fetchHtml(seedUrl);
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

      if (crawlDelayMs > 0) {
        await sleep(crawlDelayMs);
      }

      try {
        const pageResult = await processSinglePage({
          siteId,
          snapshotId,
          url: next.url,
          depth: next.depth,
          seedUrl,
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
        console.error(`[page error] ${next.url}`, err.message);
        await updateJobProgress(jobId, pagesDone, errorsCount);
      }
    }

    clearInterval(heartbeatTimer);
    await markSnapshotFinished(snapshotId);
    await completeJob(jobId, true, null);
    console.log(`[job done] id=${jobId} pages=${pagesDone} errors=${errorsCount}`);
  } catch (err) {
    clearInterval(heartbeatTimer);
    console.error(`[job failed] id=${jobId}`, err);
    await markSnapshotFailed(snapshotId, "worker_run", err.message || "Unknown crawl error");
    await completeJob(jobId, false, err.message || "Unknown crawl error");
    throw err;
  }
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
