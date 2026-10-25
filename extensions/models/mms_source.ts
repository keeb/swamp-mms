import { z } from "npm:zod@4";
import { XMLParser } from "npm:fast-xml-parser@5.8.0";

// --- Schemas ---

const ShowConfigSchema = z.object({
  name: z.string().describe("Show name to search for"),
  slug: z
    .string()
    .optional()
    .describe(
      "Canonical catalog slug. Set when the on-disk dir name diverges " +
        "from a slugified search title (e.g. Judas releases the long " +
        "Japanese title in torrent metadata but the catalog uses a short " +
        "fan-tag like 'lastame'). Threads through to dedup so disk-checks " +
        "anchor on this slug instead of the LLM-parsed title.",
    ),
  provider: z
    .enum(["subsplease", "nyaa", "eztv", "newznab"])
    .describe("Content source provider"),
  resolution: z.string().default("1080").describe(
    "Resolution filter (e.g. 1080)",
  ),
  preferGroup: z
    .string()
    .optional()
    .describe("Preferred release group (e.g. MeGusta)"),
  preferCodec: z
    .string()
    .optional()
    .describe("Preferred codec (e.g. HEVC, x265)"),
  nyaaUser: z
    .string()
    .optional()
    .describe("Nyaa uploader filter (e.g. Erai-raws)"),
  nyaaQuery: z
    .string()
    .optional()
    .describe("Override Nyaa search query (defaults to show name)"),
  eztvUrl: z
    .string()
    .optional()
    .describe("Custom EZTV RSS URL (defaults to myrss.org/eztv)"),
  eztvImdbId: z
    .string()
    .optional()
    .describe(
      "IMDb ID (tt0239195). When set, search EZTV's archival API for this " +
        "show instead of the rolling RSS feed",
    ),
  newznabUrl: z
    .string()
    .optional()
    .describe("Newznab API base URL (e.g. https://api.althub.co.za)"),
  newznabApiKey: z
    .string()
    .optional()
    .meta({ sensitive: true })
    .describe("Newznab API key"),
  newznabCat: z
    .string()
    .optional()
    .describe("Newznab category ID (e.g. 5040 for TV HD, 2040 for Movies HD)"),
  newznabSearchType: z
    .enum(["tvsearch", "search", "movie"])
    .optional()
    .describe(
      "Newznab API endpoint: tvsearch (default, TV episodes), search (general), movie",
    ),
});

const GlobalArgsSchema = z.object({
  shows: z
    .array(ShowConfigSchema)
    .optional()
    .describe("Configured shows for search_configured method"),
});

const EpisodeSchema = z.object({
  show: z.string().describe(
    "Show name (from search query for subsplease, raw for nyaa)",
  ),
  slug: z.string().optional().describe(
    "Canonical catalog slug from ShowConfig (overrides parsed title in dedup)",
  ),
  episode: z.string().optional().describe(
    "Episode number (extracted for subsplease and the EZTV API, absent for nyaa)",
  ),
  season: z.string().optional().describe(
    "Season number (EZTV API); with slug + episode, dedup checks disk without the LLM",
  ),
  magnet: z.string().describe(
    "Download URI (magnet for torrents, NZB URL for usenet)",
  ),
  provider: z.string().describe("Source provider"),
  resolution: z.string().describe("Video resolution"),
  infoHash: z.string().optional().describe("Torrent info hash"),
  publishDate: z.string().optional().describe("RSS publish date"),
  rawTitle: z.string().optional().describe("Original title from source"),
  seeders: z.number().optional().describe("Seeder count (nyaa)"),
  protocol: z.enum(["torrent", "usenet"]).optional().describe(
    "Download protocol",
  ),
  size: z.number().optional().describe("File size in bytes"),
});

// Manifest of every item from a single search invocation. Single-instance
// resource (name "current") that supersedes on each run — `data.latest(<model>,
// "episodes")` always returns exactly this run's results. No per-episode
// factory resources; the aggregate is the source of truth.
const EpisodesSchema = z.object({
  episodes: z.array(EpisodeSchema).describe("All items from this run"),
  count: z.number().describe("Number of items in this run"),
  timestamp: z.string().describe("ISO timestamp when the run finished"),
  queryEcho: z.string().optional().describe(
    "The effective query actually sent to the provider (nyaaQuery override " +
      "resolved). Lets a consumer confirm what was searched.",
  ),
  rawHitCount: z.number().optional().describe(
    "Items the provider returned BEFORE resolution/batch/dedup filtering. " +
      "A real empty (rawHitCount 0) is distinguishable from an over-filtered " +
      "one (rawHitCount > 0 but count 0).",
  ),
  providerStatus: z.string().optional().describe(
    "'ok' if the provider responded, otherwise the error. An empty " +
      "`episodes` with providerStatus 'ok' is a trustworthy absence; with an " +
      "error it is a non-answer, NOT proof the content is uncarried.",
  ),
});

// --- SubsPlease ---

interface SubsPleaseDownload {
  res: string;
  magnet: string;
}

interface SubsPleaseEntry {
  downloads: SubsPleaseDownload[];
}

async function searchSubsPlease(
  query: string,
  resolution: string,
  // deno-lint-ignore no-explicit-any
  logger: any,
  // deno-lint-ignore no-explicit-any
): Promise<{ items: any[]; rawHitCount: number }> {
  const encoded = encodeURIComponent(query);
  const url =
    `https://subsplease.org/api/?f=search&tz=America/New_York&s=${encoded}`;
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`SubsPlease API error: ${resp.status}`);

  const data = await resp.json();
  // SubsPlease returns [] when nothing matches, else an object keyed by
  // "<show> - <ep>". rawHitCount is the pre-resolution-filter entry count.
  if (Array.isArray(data)) return { items: [], rawHitCount: 0 };
  const rawHitCount = Object.keys(
    data as Record<string, SubsPleaseEntry>,
  ).length;

  const batchPattern = /\d{2}-\d{2}/;
  // deno-lint-ignore no-explicit-any
  const results: any[] = [];

  for (
    const [key, value] of Object.entries(
      data as Record<string, SubsPleaseEntry>,
    )
  ) {
    if (batchPattern.test(key)) continue;

    let episode: string;
    if (key.endsWith(" - Movie")) {
      const parts = key.replace(/ - Movie$/, "").split(" - ");
      episode = parts.length > 1 ? parts[1] : parts[0];
    } else {
      episode = key.split(" ").pop() ?? key;
    }

    const downloads = (value as SubsPleaseEntry).downloads ?? [];
    for (const dl of downloads) {
      if (dl.res === resolution) {
        results.push({
          show: query,
          episode,
          magnet: dl.magnet,
          provider: "subsplease",
          resolution,
          rawTitle: key,
        });
      }
    }
  }

  logger.info(
    `SubsPlease: found ${results.length} episodes for "${query}" (from ${rawHitCount} raw entries)`,
  );
  return { items: results, rawHitCount };
}

// --- Nyaa (general, no LLM) ---

const BATCH_PATTERN = /\d+\s*~\s*\d+/;

async function searchNyaa(
  query: string,
  resolution: string,
  // deno-lint-ignore no-explicit-any
  logger: any,
  nyaaUser?: string,
  nyaaQuery?: string,
  // deno-lint-ignore no-explicit-any
): Promise<{ items: any[]; rawHitCount: number }> {
  const searchTerm = nyaaQuery ?? query;
  const params = new URLSearchParams({
    page: "rss",
    q: searchTerm,
    c: "1_0",
    f: "0",
  });
  if (nyaaUser) params.set("u", nyaaUser);

  const url = `https://nyaa.si/?${params}`;
  logger.info(`Nyaa RSS: ${url}`);

  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`Nyaa RSS error: ${resp.status}`);

  const xml = await resp.text();
  const parser = new XMLParser({
    ignoreAttributes: false,
    removeNSPrefix: false,
  });
  const feed = parser.parse(xml);
  const items = feed?.rss?.channel?.item ?? [];
  const itemList = Array.isArray(items) ? items : [items];

  const normalizedRes = resolution.replace(/p$/, "");
  // deno-lint-ignore no-explicit-any
  const results: any[] = [];

  for (const item of itemList) {
    const title = item.title;
    const infoHash = item["nyaa:infoHash"];
    if (!title || !infoHash) continue;

    // Skip batches
    if (BATCH_PATTERN.test(title)) continue;

    // Quick resolution filter
    if (!title.includes(`${normalizedRes}p`)) continue;

    const encodedTitle = encodeURIComponent(title);
    const magnet = `magnet:?xt=urn:btih:${infoHash}&dn=${encodedTitle}`;
    const seeders = parseInt(item["nyaa:seeders"]) || 0;

    results.push({
      show: query,
      magnet,
      provider: "nyaa",
      resolution: `${normalizedRes}p`,
      infoHash,
      rawTitle: title,
      publishDate: item.pubDate,
      seeders,
    });
  }

  // Deduplicate by infoHash
  const seen = new Set<string>();
  const deduped = results.filter((r) => {
    if (seen.has(r.infoHash)) return false;
    seen.add(r.infoHash);
    return true;
  });

  logger.info(
    `Nyaa: ${deduped.length} items after filter+dedup (from ${itemList.length} raw)`,
  );
  return { items: deduped, rawHitCount: itemList.length };
}

// --- EZTV ---

const EZTV_DEFAULT_URL = "https://myrss.org/eztv";

async function fetchEztvFeed(
  // deno-lint-ignore no-explicit-any
  logger: any,
  eztvUrl?: string,
  // deno-lint-ignore no-explicit-any
): Promise<any[]> {
  const url = eztvUrl ?? EZTV_DEFAULT_URL;
  logger.info(`EZTV RSS: ${url}`);

  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`EZTV RSS error: ${resp.status}`);

  const xml = await resp.text();
  const parser = new XMLParser({
    ignoreAttributes: false,
    removeNSPrefix: false,
    isArray: (_name: string, jpath: string) => jpath === "rss.channel.item",
  });
  const feed = parser.parse(xml);
  return feed?.rss?.channel?.item ?? [];
}

function matchEztvItems(
  // deno-lint-ignore no-explicit-any
  items: any[],
  query: string,
  resolution: string,
  // deno-lint-ignore no-explicit-any
  logger: any,
  preferGroup?: string,
  preferCodec?: string,
  // deno-lint-ignore no-explicit-any
): any[] {
  const normalizedRes = resolution.replace(/p$/, "");
  const normalizedQuery = query.toLowerCase();
  // deno-lint-ignore no-explicit-any
  const results: any[] = [];

  for (const item of items) {
    const title = item.title ?? item["torrent:fileName"];
    if (!title) continue;

    // API items are already scoped to the show by IMDb ID
    if (!item._api && !title.toLowerCase().includes(normalizedQuery)) continue;
    if (
      !title.includes(`${normalizedRes}p`) && !title.includes(normalizedRes)
    ) continue;

    let magnet = item["torrent:magnetURI"] ?? "";
    if (typeof magnet === "object") magnet = magnet["#text"] ?? "";
    magnet = magnet.trim();

    const infoHash = item["torrent:infoHash"] ?? "";
    if (!magnet && infoHash) {
      magnet = `magnet:?xt=urn:btih:${infoHash}&dn=${
        encodeURIComponent(title)
      }`;
    }
    if (!magnet) continue;

    const seeders = parseInt(item["torrent:seeds"]) || 0;

    // Quality scoring
    const hasGroup = preferGroup ? title.includes(preferGroup) : false;
    const hasCodec = preferCodec
      ? title.includes(preferCodec) || title.includes(preferCodec.toLowerCase())
      : false;
    const score = (hasGroup ? 10 : 0) + (hasCodec ? 5 : 0);

    results.push({
      show: query,
      magnet,
      provider: "eztv",
      resolution: `${normalizedRes}p`,
      infoHash: infoHash || undefined,
      rawTitle: title,
      publishDate: item.pubDate,
      seeders,
      season: item._season,
      episode: item._episode,
      size: item._size,
      _score: score,
    });
  }

  // Deduplicate by infoHash, keep highest scored
  results.sort((a, b) => b._score - a._score);
  const seen = new Set<string>();
  const deduped = results.filter((r) => {
    if (!r.infoHash) return true;
    if (seen.has(r.infoHash)) return false;
    seen.add(r.infoHash);
    return true;
  });

  // If we have quality prefs, only keep the best release per episode
  if (preferGroup || preferCodec) {
    // Extract S01E09 style episode key from title
    // deno-lint-ignore no-explicit-any
    const byEpisode = new Map<string, any>();
    for (const r of deduped) {
      const epMatch = r.rawTitle.match(/S\d+E\d+/i);
      const key = epMatch ? epMatch[0].toUpperCase() : r.rawTitle;
      const existing = byEpisode.get(key);
      if (!existing || r._score > existing._score) {
        byEpisode.set(key, r);
      }
    }
    const best = [...byEpisode.values()].map(({ _score, ...rest }) => rest);
    logger.info(
      `EZTV: ${best.length} best matches for "${query}" at ${normalizedRes}p (preferred: ${
        preferGroup ?? ""
      } ${preferCodec ?? ""}) from ${items.length} raw`,
    );
    return best;
  }

  const clean = deduped.map(({ _score, ...rest }) => rest);
  logger.info(
    `EZTV: ${clean.length} items matching "${query}" at ${normalizedRes}p (from ${items.length} raw)`,
  );
  return clean;
}

// EZTV's JSON API keeps every torrent for a show; the RSS feed only the
// latest ~30 across all shows. Mirrors tried in order.
const EZTV_API_MIRRORS = ["https://eztvx.to", "https://eztv.wf"];
const EZTV_API_PAGE = 100;

/** `SxxEyy` key for an episode, used to match the onlyEpisodes filter. */
export function episodeKey(
  season: string | number | undefined,
  episode: string | number | undefined,
): string | null {
  const s = parseInt(String(season ?? ""));
  const e = parseInt(String(episode ?? ""));
  return Number.isFinite(s) && Number.isFinite(e) && e > 0
    ? `S${s}E${e}`
    : null;
}

/** `SxxEyy` key from a release title (RSS items carry no season/episode). */
export function episodeKeyFromTitle(title?: string): string | null {
  const m = title?.match(/S(\d+)E(\d+)/i);
  return m ? episodeKey(m[1], m[2]) : null;
}

/**
 * Map an EZTV API torrent into the RSS item shape matchEztvItems reads. The
 * title is the release filename without extension (what lands on disk), and
 * season/episode ride along so dedup needs no LLM. Season packs (episode 0)
 * carry no episode.
 */
// deno-lint-ignore no-explicit-any
export function eztvApiItem(t: any): any {
  const title = t.filename
    ? String(t.filename).replace(/\.[a-z0-9]{2,4}$/i, "")
    : String(t.title ?? "");
  const hasEpisode = episodeKey(t.season, t.episode) !== null;
  return {
    _api: true,
    title,
    "torrent:magnetURI": t.magnet_url ?? "",
    "torrent:infoHash": t.hash ?? "",
    "torrent:seeds": String(t.seeds ?? 0),
    pubDate: t.date_released_unix
      ? new Date(Number(t.date_released_unix) * 1000).toISOString()
      : undefined,
    _season: hasEpisode ? String(parseInt(t.season)) : undefined,
    _episode: hasEpisode ? String(parseInt(t.episode)) : undefined,
    _size: Number(t.size_bytes) || undefined,
  };
}

/**
 * Newest-first pages of a show's EZTV torrents. With `wanted` episode keys,
 * keeps paging (up to maxPages) until every one has turned up; without, one
 * page.
 */
async function fetchEztvApi(
  imdbId: string,
  wanted: Set<string> | null,
  // deno-lint-ignore no-explicit-any
  logger: any,
  maxPages = 20,
  // deno-lint-ignore no-explicit-any
): Promise<any[]> {
  const id = imdbId.replace(/^tt/i, "");
  let lastErr: unknown = null;
  for (const base of EZTV_API_MIRRORS) {
    try {
      // deno-lint-ignore no-explicit-any
      const items: any[] = [];
      const seen = new Set<string>();
      const pages = wanted ? maxPages : 1;
      for (let page = 1; page <= pages; page++) {
        const url =
          `${base}/api/get-torrents?imdb_id=${id}&limit=${EZTV_API_PAGE}&page=${page}`;
        logger.info(`EZTV API: ${url}`);
        const resp = await fetch(url);
        if (!resp.ok) throw new Error(`EZTV API error: ${resp.status}`);
        const json = await resp.json();
        const torrents = Array.isArray(json?.torrents) ? json.torrents : [];
        for (const t of torrents) {
          const item = eztvApiItem(t);
          items.push(item);
          const key = episodeKey(item._season, item._episode);
          if (key) seen.add(key);
        }
        if (torrents.length < EZTV_API_PAGE) break;
        if (wanted && [...wanted].every((k) => seen.has(k))) break;
      }
      return items;
    } catch (err) {
      lastErr = err;
      logger.warning(`EZTV API mirror ${base} failed: ${err}`);
    }
  }
  throw lastErr instanceof Error ? lastErr : new Error(String(lastErr));
}

async function searchEztv(
  query: string,
  resolution: string,
  // deno-lint-ignore no-explicit-any
  logger: any,
  eztvUrl?: string,
  preferGroup?: string,
  preferCodec?: string,
  eztvImdbId?: string,
  wanted: Set<string> | null = null,
  // deno-lint-ignore no-explicit-any
): Promise<{ items: any[]; rawHitCount: number }> {
  const feedItems = eztvImdbId
    ? await fetchEztvApi(eztvImdbId, wanted, logger)
    : await fetchEztvFeed(logger, eztvUrl);
  const items = matchEztvItems(
    feedItems,
    query,
    resolution,
    logger,
    preferGroup,
    preferCodec,
  );
  return { items, rawHitCount: feedItems.length };
}

// --- Newznab (usenet indexers like althub, nzbgeek, etc.) ---

async function searchNewznab(
  query: string,
  resolution: string,
  apiUrl: string,
  apiKey: string,
  // deno-lint-ignore no-explicit-any
  logger: any,
  category?: string,
  searchType?: "tvsearch" | "search" | "movie",
  // deno-lint-ignore no-explicit-any
): Promise<{ items: any[]; rawHitCount: number }> {
  // Default to general `search` so movies, TV, and uncategorized releases all
  // come back. Callers can narrow with searchType=tvsearch (cat 5040 default)
  // or searchType=movie (cat 2040 default).
  const t = searchType ?? "search";
  const defaultCat = t === "tvsearch"
    ? "5040"
    : t === "movie"
    ? "2040"
    : undefined;
  const cat = category ?? defaultCat;
  const params = new URLSearchParams({
    t,
    q: query,
    apikey: apiKey,
    limit: "100",
  });
  if (cat) params.set("cat", cat);

  const url = `${apiUrl}/api?${params}`;
  logger.info(
    `Newznab: ${apiUrl}/api?t=${t}&q=${encodeURIComponent(query)}${
      cat ? `&cat=${cat}` : ""
    }`,
  );

  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`Newznab API error: ${resp.status}`);

  const xml = await resp.text();
  const parser = new XMLParser({
    ignoreAttributes: false,
    removeNSPrefix: false,
    isArray: (_name: string, jpath: string) =>
      jpath === "rss.channel.item" || jpath.endsWith(".newznab:attr"),
  });
  const feed = parser.parse(xml);
  const items = feed?.rss?.channel?.item ?? [];

  const normalizedRes = resolution.replace(/p$/, "");
  // deno-lint-ignore no-explicit-any
  const results: any[] = [];

  for (const item of items) {
    const title = item.title;
    if (!title) continue;

    // Resolution filter
    if (
      !title.includes(`${normalizedRes}p`) && !title.includes(normalizedRes)
    ) continue;

    // Prefer MeGusta HEVC releases
    const isMegusta = title.includes("MeGusta");
    const isHevc = title.includes("HEVC") || title.includes("x265");

    // NZB download URL
    let nzbUrl = item.link ?? "";
    if (typeof nzbUrl === "object") nzbUrl = nzbUrl["#text"] ?? "";

    // Extract guid for dedup
    let guid = "";
    if (typeof item.guid === "string") {
      guid = item.guid;
    } else if (item.guid?.["#text"]) {
      guid = item.guid["#text"];
    }
    // Extract guid hash from URL
    const guidMatch = guid.match(/([a-f0-9]{32})/);
    const guidHash = guidMatch ? guidMatch[1] : "";

    // Extract size from newznab attributes
    let size = 0;
    const enclosure = item.enclosure;
    if (enclosure?.["@_length"]) {
      size = parseInt(enclosure["@_length"]) || 0;
    }

    if (!nzbUrl) continue;

    results.push({
      show: query,
      magnet: nzbUrl, // NZB URL goes in the magnet field
      provider: "newznab",
      resolution: `${normalizedRes}p`,
      rawTitle: title,
      publishDate: item.pubDate,
      protocol: "usenet",
      size,
      infoHash: guidHash || undefined,
      _isMegusta: isMegusta,
      _isHevc: isHevc,
      _score: (isMegusta ? 10 : 0) + (isHevc ? 5 : 0),
    });
  }

  // Sort by preference: MeGusta HEVC first, then HEVC, then rest
  results.sort((a, b) => b._score - a._score);

  // Deduplicate by title (keep highest scored)
  const seen = new Set<string>();
  const deduped = results.filter((r) => {
    const key = r.rawTitle;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  }).map(({ _isMegusta, _isHevc, _score, ...rest }) => rest);

  logger.info(
    `Newznab: ${deduped.length} items for "${query}" at ${normalizedRes}p (from ${items.length} raw)`,
  );
  return { items: deduped, rawHitCount: items.length };
}

// --- Model ---

export const model = {
  type: "@keeb/mms/source",
  version: "2026.10.25.1",
  reports: ["@keeb/mms/discovery-summary"],
  globalArguments: GlobalArgsSchema,
  upgrades: [
    {
      fromVersion: "2026.03.28.1",
      toVersion: "2026.03.28.2",
      description:
        "Replace erai-raws provider with general nyaa provider, add ollama config",
      // deno-lint-ignore no-explicit-any
      upgradeAttributes: (old: any) => ({
        ...old,
        // deno-lint-ignore no-explicit-any
        shows: old.shows?.map((s: any) => ({
          ...s,
          provider: s.provider === "erai-raws" ? "nyaa" : s.provider,
          nyaaUser: s.provider === "erai-raws" ? "Erai-raws" : s.nyaaUser,
        })),
      }),
    },
    {
      fromVersion: "2026.03.28.2",
      toVersion: "2026.03.29.1",
      description:
        "Remove inline ollama — LLM parsing happens via @keeb/ollama model in workflow",
      // deno-lint-ignore no-explicit-any
      upgradeAttributes: (old: any) => {
        const { ollamaUrl: _ollamaUrl, ollamaModel: _ollamaModel, ...rest } =
          old;
        return rest;
      },
    },
    {
      fromVersion: "2026.03.29.1",
      toVersion: "2026.03.29.2",
      description: "Add EZTV provider for western TV shows",
      // deno-lint-ignore no-explicit-any
      upgradeAttributes: (old: any) => old,
    },
    {
      fromVersion: "2026.03.29.2",
      toVersion: "2026.03.29.3",
      description:
        "Add Newznab provider for usenet indexers (althub, nzbgeek, etc.)",
      // deno-lint-ignore no-explicit-any
      upgradeAttributes: (old: any) => old,
    },
    {
      fromVersion: "2026.03.29.3",
      toVersion: "2026.03.30.1",
      description:
        "Add preferGroup/preferCodec quality scoring for EZTV, single-pull optimization",
      // deno-lint-ignore no-explicit-any
      upgradeAttributes: (old: any) => old,
    },
    {
      fromVersion: "2026.03.30.1",
      toVersion: "2026.04.08.1",
      description:
        "Add `batch` resource spec — single 'latest' instance written per " +
        "invocation, supersedes the previous version. Lets dedup scope to " +
        "exactly the latest run's results without catalog accumulation.",
      // deno-lint-ignore no-explicit-any
      upgradeAttributes: (old: any) => old,
    },
    {
      fromVersion: "2026.04.08.1",
      toVersion: "2026.04.11.1",
      description:
        "Append content hash to slug when truncated past 80 chars so " +
        "long show names with episode-number suffixes don't collide.",
      // deno-lint-ignore no-explicit-any
      upgradeAttributes: (old: any) => old,
    },
    {
      fromVersion: "2026.04.11.1",
      toVersion: "2026.04.14.1",
      description:
        "Drop per-episode factory resources — search results are pipeline " +
        "output (a materialized view of the upstream catalog), not durable " +
        "entities. The aggregate resource (now renamed `episodes`) is the " +
        "sole output and source of truth. Orphaned `episode` records from " +
        "prior versions can be purged with `swamp data gc`.",
      // deno-lint-ignore no-explicit-any
      upgradeAttributes: (old: any) => old,
    },
    {
      fromVersion: "2026.04.14.1",
      toVersion: "2026.05.04.1",
      description:
        "Make Newznab search endpoint configurable (newznabSearchType: " +
        "tvsearch | search | movie). Default remains tvsearch.",
      // deno-lint-ignore no-explicit-any
      upgradeAttributes: (old: any) => old,
    },
    {
      fromVersion: "2026.05.04.1",
      toVersion: "2026.05.08.1",
      description:
        "Add optional `slug` to ShowConfig. When set, threads through into " +
        "each episode so dedup can anchor disk-checks on the canonical " +
        "catalog slug instead of an LLM-parsed title (needed for release " +
        "groups like Judas where the torrent's display title and the file " +
        "tag diverge).",
      // deno-lint-ignore no-explicit-any
      upgradeAttributes: (old: any) => old,
    },
    {
      fromVersion: "2026.05.08.1",
      toVersion: "2026.07.09.1",
      description:
        "Add a provenance envelope to the `episodes` resource from the " +
        "`search` method: queryEcho (effective query sent), rawHitCount " +
        "(provider items before filtering), providerStatus. Lets a consumer " +
        "tell a trustworthy empty (providerStatus 'ok', rawHitCount 0) from " +
        "an over-filtered or failed one. globalArguments unchanged.",
      // deno-lint-ignore no-explicit-any
      upgradeAttributes: (old: any) => old,
    },
    {
      fromVersion: "2026.07.09.1",
      toVersion: "2026.10.25.1",
      description:
        "EZTV archival search: optional `eztvImdbId` (ShowConfig and search) " +
        "uses EZTV's JSON API instead of the rolling RSS feed, and items " +
        "carry season/episode. `search` gains `slug` and `onlyEpisodes` " +
        "(keep just the episodes dedup reported missing).",
      // deno-lint-ignore no-explicit-any
      upgradeAttributes: (old: any) => old,
    },
  ],
  resources: {
    episodes: {
      description:
        "All items from the most recent invocation. Single instance named " +
        "'current' that supersedes on each run — `data.latest(<model>, " +
        '"episodes")` always returns this run\'s results and only this ' +
        "run's results.",
      schema: EpisodesSchema,
      lifetime: "infinite" as const,
      garbageCollection: 5,
    },
  },
  methods: {
    search: {
      description:
        "Search a provider for content. Writes a single `episodes` resource " +
        "containing all items from this run.",
      arguments: z.object({
        query: z.string().describe("Show name to search for"),
        provider: z.enum(["subsplease", "nyaa", "eztv", "newznab"]).describe(
          "Source provider",
        ),
        resolution: z.string().default("1080").describe("Resolution filter"),
        preferGroup: z.string().optional().describe(
          "Preferred release group (e.g. MeGusta)",
        ),
        preferCodec: z.string().optional().describe(
          "Preferred codec (e.g. HEVC)",
        ),
        nyaaUser: z.string().optional().describe("Nyaa uploader filter"),
        nyaaQuery: z.string().optional().describe("Override search query"),
        eztvUrl: z.string().optional().describe("Custom EZTV RSS URL"),
        eztvImdbId: z.string().optional().describe(
          "IMDb ID (tt0239195): search EZTV's archival API instead of the rolling RSS feed",
        ),
        slug: z.string().optional().describe(
          "Canonical catalog slug, stamped on every item (dedup checks disk at /<slug>/s<N>)",
        ),
        onlyEpisodes: z.array(z.object({
          season: z.union([z.string(), z.number()]).optional(),
          episode: z.union([z.string(), z.number()]).optional(),
        })).optional().describe(
          "Keep only these episodes, e.g. dedup's missing items; anything else is dropped",
        ),
        newznabUrl: z.string().optional().describe("Newznab API base URL"),
        newznabApiKey: z.string().optional().meta({ sensitive: true }).describe(
          "Newznab API key",
        ),
        newznabCat: z.string().optional().describe("Newznab category ID"),
        newznabSearchType: z
          .enum(["tvsearch", "search", "movie"])
          .optional()
          .describe(
            "Newznab API endpoint (default: search — broad across all categories)",
          ),
      }),
      execute: async (
        args: {
          query: string;
          provider: "subsplease" | "nyaa" | "eztv" | "newznab";
          resolution: string;
          preferGroup?: string;
          preferCodec?: string;
          nyaaUser?: string;
          nyaaQuery?: string;
          eztvUrl?: string;
          eztvImdbId?: string;
          slug?: string;
          onlyEpisodes?: {
            season?: string | number;
            episode?: string | number;
          }[];
          newznabUrl?: string;
          newznabApiKey?: string;
          newznabCat?: string;
          newznabSearchType?: "tvsearch" | "search" | "movie";
        },
        // deno-lint-ignore no-explicit-any
        context: any,
      ) => {
        const wanted = args.onlyEpisodes
          ? new Set(
            args.onlyEpisodes
              .map((e) => episodeKey(e.season, e.episode))
              .filter((k): k is string => k !== null),
          )
          : null;
        // deno-lint-ignore no-explicit-any
        let items: any[] = [];
        let rawHitCount = 0;
        let providerStatus = "ok";
        // Echo the query actually sent so a consumer can tell an empty result
        // apart from a query that never reached the provider. Only nyaa
        // rewrites the query (nyaaQuery override); the rest search verbatim.
        const queryEcho = args.provider === "nyaa"
          ? (args.nyaaQuery ?? args.query)
          : args.query;
        try {
          const result = args.provider === "subsplease"
            ? await searchSubsPlease(
              args.query,
              args.resolution,
              context.logger,
            )
            : args.provider === "eztv"
            ? await searchEztv(
              args.query,
              args.resolution,
              context.logger,
              args.eztvUrl,
              args.preferGroup,
              args.preferCodec,
              args.eztvImdbId,
              wanted,
            )
            : args.provider === "newznab"
            ? await searchNewznab(
              args.query,
              args.resolution,
              args.newznabUrl!,
              args.newznabApiKey!,
              context.logger,
              args.newznabCat,
              args.newznabSearchType,
            )
            : await searchNyaa(
              args.query,
              args.resolution,
              context.logger,
              args.nyaaUser,
              args.nyaaQuery,
            );
          items = result.items;
          rawHitCount = result.rawHitCount;
          if (wanted) {
            items = items.filter((it) => {
              const key = episodeKey(it.season, it.episode) ??
                episodeKeyFromTitle(it.rawTitle);
              return key !== null && wanted.has(key);
            });
            context.logger.info(
              `Kept ${items.length} item(s) for ${wanted.size} wanted episode(s): ${
                [...wanted].join(", ")
              }`,
            );
          }
          if (args.slug) {
            items = items.map((it) => ({ ...it, slug: args.slug }));
          }
        } catch (err) {
          // Don't throw: a provider failure must surface as a legible
          // non-answer (empty episodes + providerStatus=error), not a crash a
          // consumer could misread as "nothing is carried".
          providerStatus = err instanceof Error ? err.message : String(err);
          context.logger.error(
            `search failed for "${args.query}" on ${args.provider}: ${providerStatus}`,
          );
        }

        const handle = await context.writeResource("episodes", "current", {
          episodes: items,
          count: items.length,
          timestamp: new Date().toISOString(),
          queryEcho,
          rawHitCount,
          providerStatus,
        });
        return { dataHandles: [handle] };
      },
    },

    search_configured: {
      description:
        "Search all configured shows. Writes a single `episodes` resource " +
        "containing every item discovered across all shows in this run.",
      arguments: z.object({}),
      // deno-lint-ignore no-explicit-any
      execute: async (_args: Record<string, never>, context: any) => {
        const shows = context.globalArgs.shows ?? [];
        if (shows.length === 0) {
          context.logger.info("No shows configured");
          return { dataHandles: [] };
        }

        // Accumulate every item across every show so we can write a single
        // aggregate resource at the end.
        // deno-lint-ignore no-explicit-any
        const allItems: any[] = [];

        // Pull EZTV feed once for all eztv shows
        // deno-lint-ignore no-explicit-any
        const eztvShows = shows.filter((s: any) =>
          s.provider === "eztv" && !s.eztvImdbId
        );
        // deno-lint-ignore no-explicit-any
        let eztvFeedItems: any[] | null = null;
        if (eztvShows.length > 0) {
          try {
            eztvFeedItems = await fetchEztvFeed(
              context.logger,
              eztvShows[0].eztvUrl,
            );
          } catch (err) {
            context.logger.error(`Failed to fetch EZTV feed: ${err}`);
          }
        }

        for (const show of shows) {
          try {
            // deno-lint-ignore no-explicit-any
            let items: any[];
            if (show.provider === "eztv" && show.eztvImdbId) {
              items = (await searchEztv(
                show.name,
                show.resolution ?? "1080",
                context.logger,
                show.eztvUrl,
                show.preferGroup,
                show.preferCodec,
                show.eztvImdbId,
              )).items;
            } else if (show.provider === "eztv" && eztvFeedItems !== null) {
              items = matchEztvItems(
                eztvFeedItems,
                show.name,
                show.resolution ?? "1080",
                context.logger,
                show.preferGroup,
                show.preferCodec,
              );
            } else if (show.provider === "subsplease") {
              items = (await searchSubsPlease(
                show.name,
                show.resolution ?? "1080",
                context.logger,
              )).items;
            } else if (show.provider === "newznab") {
              items = (await searchNewznab(
                show.name,
                show.resolution ?? "1080",
                show.newznabUrl!,
                show.newznabApiKey!,
                context.logger,
                show.newznabCat,
                show.newznabSearchType,
              )).items;
            } else if (show.provider === "nyaa") {
              items = (await searchNyaa(
                show.name,
                show.resolution ?? "1080",
                context.logger,
                show.nyaaUser,
                show.nyaaQuery,
              )).items;
            } else {
              continue;
            }

            if (show.slug) {
              items = items.map((it) => ({ ...it, slug: show.slug }));
            }

            allItems.push(...items);
          } catch (err) {
            context.logger.error(
              `Failed "${show.name}" on ${show.provider}: ${err}`,
            );
          }
        }

        const handle = await context.writeResource("episodes", "current", {
          episodes: allItems,
          count: allItems.length,
          timestamp: new Date().toISOString(),
        });

        context.logger.info(
          `Total: ${allItems.length} items across ${shows.length} shows`,
        );
        return { dataHandles: [handle] };
      },
    },
  },
};
