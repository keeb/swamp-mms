import { z } from "npm:zod@4";
import { XMLParser } from "npm:fast-xml-parser@4.5.1";

// --- Schemas ---

const ShowConfigSchema = z.object({
  name: z.string().describe("Show name to search for"),
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
  episode: z.string().optional().describe(
    "Episode number (extracted for subsplease, absent for nyaa)",
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
): Promise<any[]> {
  const encoded = encodeURIComponent(query);
  const url =
    `https://subsplease.org/api/?f=search&tz=America/New_York&s=${encoded}`;
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`SubsPlease API error: ${resp.status}`);

  const data = await resp.json();
  if (Array.isArray(data)) return [];

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

  logger.info(`SubsPlease: found ${results.length} episodes for "${query}"`);
  return results;
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
): Promise<any[]> {
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
  return deduped;
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

    if (!title.toLowerCase().includes(normalizedQuery)) continue;
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

async function searchEztv(
  query: string,
  resolution: string,
  // deno-lint-ignore no-explicit-any
  logger: any,
  eztvUrl?: string,
  preferGroup?: string,
  preferCodec?: string,
  // deno-lint-ignore no-explicit-any
): Promise<any[]> {
  const items = await fetchEztvFeed(logger, eztvUrl);
  return matchEztvItems(
    items,
    query,
    resolution,
    logger,
    preferGroup,
    preferCodec,
  );
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
): Promise<any[]> {
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
  return deduped;
}

// --- Model ---

export const model = {
  type: "@keeb/mms/source",
  version: "2026.05.04.1",
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
  ],
  resources: {
    episodes: {
      description:
        "All items from the most recent invocation. Single instance named " +
        "'current' that supersedes on each run — `data.latest(<model>, " +
        "\"episodes\")` always returns this run's results and only this " +
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
          newznabUrl?: string;
          newznabApiKey?: string;
          newznabCat?: string;
          newznabSearchType?: "tvsearch" | "search" | "movie";
        },
        // deno-lint-ignore no-explicit-any
        context: any,
      ) => {
        const items = args.provider === "subsplease"
          ? await searchSubsPlease(args.query, args.resolution, context.logger)
          : args.provider === "eztv"
          ? await searchEztv(
            args.query,
            args.resolution,
            context.logger,
            args.eztvUrl,
            args.preferGroup,
            args.preferCodec,
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

        const handle = await context.writeResource("episodes", "current", {
          episodes: items,
          count: items.length,
          timestamp: new Date().toISOString(),
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
        const eztvShows = shows.filter((s: any) => s.provider === "eztv");
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
            if (show.provider === "eztv" && eztvFeedItems !== null) {
              items = matchEztvItems(
                eztvFeedItems,
                show.name,
                show.resolution ?? "1080",
                context.logger,
                show.preferGroup,
                show.preferCodec,
              );
            } else if (show.provider === "subsplease") {
              items = await searchSubsPlease(
                show.name,
                show.resolution ?? "1080",
                context.logger,
              );
            } else if (show.provider === "newznab") {
              items = await searchNewznab(
                show.name,
                show.resolution ?? "1080",
                show.newznabUrl!,
                show.newznabApiKey!,
                context.logger,
                show.newznabCat,
                show.newznabSearchType,
              );
            } else if (show.provider === "nyaa") {
              items = await searchNyaa(
                show.name,
                show.resolution ?? "1080",
                context.logger,
                show.nyaaUser,
                show.nyaaQuery,
              );
            } else {
              continue;
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
