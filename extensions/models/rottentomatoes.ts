import { z } from "npm:zod@4";

// ── Schemas ─────────────────────────────────────────────────────────────────

const GlobalArgs = z.object({
  tmdbApiKey: z.string({ sensitive: true }).describe("TMDB API key (v3)"),
  headless: z.boolean().default(true).describe(
    "Run Playwright headless. Consumer repo must have playwright installed (npm install playwright) so node can resolve it via upward module search from the pulled extension.",
  ),
});

const BrowseArgs = z.object({
  sort: z.enum(["newest", "popular"]).describe(
    "Which Rotten Tomatoes TV browse list to scrape",
  ),
  loadMoreClicks: z.number().int().min(0).default(0).describe(
    "How many times to click Load More before extracting (0 = first page only)",
  ),
});

const ShowSchema = z.object({
  // Identity
  tmdbId: z.number().nullable().describe(
    "TMDB ID, null if no TMDB match was found",
  ),
  rtSlug: z.string().describe(
    "Rotten Tomatoes slug, e.g. 'the_terror_devil_in_silver'",
  ),
  rtUrl: z.string().describe("Full Rotten Tomatoes URL"),
  // RT
  rtTitle: z.string(),
  rtYear: z.number().nullable(),
  tomatoScore: z.number().nullable().describe(
    "Tomatometer / critic score (0-100)",
  ),
  popcornScore: z.number().nullable().describe(
    "Audience / popcorn score (0-100)",
  ),
  browseSort: z.enum(["newest", "popular"]).describe(
    "Which RT browse list this was last seen on",
  ),
  // TMDB enrichment (null if no match)
  tmdbName: z.string().nullable(),
  tmdbOriginalName: z.string().nullable(),
  firstAirDate: z.string().nullable(),
  status: z.string().nullable(),
  inProduction: z.boolean().nullable(),
  numberOfSeasons: z.number().nullable(),
  numberOfEpisodes: z.number().nullable(),
  networks: z.array(z.string()).default([]),
  genres: z.array(z.string()).default([]),
  overview: z.string().nullable(),
  tmdbPopularity: z.number().nullable(),
  tmdbVoteAverage: z.number().nullable(),
  tmdbVoteCount: z.number().nullable(),
  homepage: z.string().nullable(),
  posterPath: z.string().nullable(),
  collectedAt: z.string().describe("ISO timestamp of this scrape"),
});

// ── Helpers ─────────────────────────────────────────────────────────────────

async function runScraper(
  scriptPath: string,
  repoDir: string,
  method: string,
  headless: boolean,
  methodArgs: Record<string, unknown>,
) {
  const cmd = new Deno.Command("node", {
    args: [scriptPath, method, String(headless), JSON.stringify(methodArgs)],
    env: { NODE_PATH: `${repoDir}/node_modules` },
    stdout: "piped",
    stderr: "piped",
  });
  const { code, stdout, stderr } = await cmd.output();
  const stderrText = new TextDecoder().decode(stderr);
  if (stderrText) console.log(stderrText.trim());
  if (code !== 0) {
    throw new Error(`Scraper failed (exit ${code}): ${stderrText}`);
  }
  return JSON.parse(new TextDecoder().decode(stdout));
}

// Strip year suffix like "_2023" from an RT slug to derive a clean search query.
function slugToQuery(slug: string): string {
  return slug.replace(/_\d{4}$/, "").replace(/_/g, " ").trim();
}

type TmdbSearchHit = {
  id: number;
  name: string;
  original_name: string;
  first_air_date?: string;
  popularity?: number;
  vote_average?: number;
  vote_count?: number;
};

type TmdbDetails = {
  id: number;
  name: string;
  original_name: string;
  first_air_date?: string;
  status?: string;
  in_production?: boolean;
  number_of_seasons?: number;
  number_of_episodes?: number;
  networks?: { name: string }[];
  genres?: { name: string }[];
  overview?: string;
  popularity?: number;
  vote_average?: number;
  vote_count?: number;
  homepage?: string;
  poster_path?: string;
};

async function tmdbSearch(
  apiKey: string,
  query: string,
  year: number | null,
): Promise<TmdbSearchHit | null> {
  const params = new URLSearchParams({ api_key: apiKey, query });
  if (year) params.set("first_air_date_year", String(year));
  const resp = await fetch(`https://api.themoviedb.org/3/search/tv?${params}`);
  if (!resp.ok) {
    console.log(
      `[rottentomatoes] tmdb search HTTP ${resp.status} for query="${query}" year=${year}`,
    );
    return null;
  }
  const json = await resp.json() as { results?: TmdbSearchHit[] };
  if (!json.results || json.results.length === 0) {
    // Retry without year if a year was specified
    if (year) return await tmdbSearch(apiKey, query, null);
    return null;
  }
  return json.results[0];
}

async function tmdbDetails(
  apiKey: string,
  id: number,
): Promise<TmdbDetails | null> {
  const resp = await fetch(
    `https://api.themoviedb.org/3/tv/${id}?api_key=${apiKey}`,
  );
  if (!resp.ok) {
    console.log(
      `[rottentomatoes] tmdb details HTTP ${resp.status} for id=${id}`,
    );
    return null;
  }
  return await resp.json() as TmdbDetails;
}

// Limit concurrent TMDB requests so we don't hammer the API. TMDB's published
// per-second limit is generous, but small concurrency keeps logs readable and
// behaves well on transient errors.
async function mapWithConcurrency<T, R>(
  items: T[],
  limit: number,
  fn: (item: T, idx: number) => Promise<R>,
): Promise<R[]> {
  const results: R[] = new Array(items.length);
  let cursor = 0;
  const workers = Array.from(
    { length: Math.min(limit, items.length) },
    async () => {
      while (true) {
        const i = cursor++;
        if (i >= items.length) return;
        results[i] = await fn(items[i], i);
      }
    },
  );
  await Promise.all(workers);
  return results;
}

// ── Model ───────────────────────────────────────────────────────────────────

export const model = {
  type: "@keeb/rottentomatoes",
  version: "2026.05.22.1",
  resources: {
    show: {
      description:
        "A single TV show: Rotten Tomatoes scores + TMDB metadata. Spec is the TMDB ID (or rt-<slug> when no match).",
      schema: ShowSchema,
      lifetime: "infinite",
      garbageCollection: 30,
    },
  },
  globalArguments: GlobalArgs,
  methods: {
    browse: {
      description:
        "Scrape a Rotten Tomatoes TV browse list, enrich each show via TMDB, and write one resource per show keyed by TMDB ID.",
      arguments: BrowseArgs,
      execute: async (args, context) => {
        const { tmdbApiKey, headless } = context.globalArgs;
        console.log(
          `[rottentomatoes/browse] sort=${args.sort} loadMoreClicks=${args.loadMoreClicks}`,
        );

        const scriptPath = context.extensionFile(
          "scripts/rottentomatoes-scraper.ts",
        );
        const scraped = await runScraper(
          scriptPath,
          context.repoDir,
          "browse",
          headless,
          {
            sort: args.sort,
            loadMoreClicks: args.loadMoreClicks,
          },
        );

        const collectedAt = scraped.collectedAt;
        const items = scraped.items as Array<
          {
            title: string;
            year: number | null;
            slug: string;
            url: string;
            tomatoScore: number | null;
            popcornScore: number | null;
          }
        >;
        console.log(
          `[rottentomatoes/browse] scraped ${items.length} tiles; enriching via TMDB`,
        );

        const enriched = await mapWithConcurrency(items, 4, async (it) => {
          const query = slugToQuery(it.slug) || it.title;
          let details: TmdbDetails | null = null;
          try {
            const hit = await tmdbSearch(tmdbApiKey, query, it.year);
            if (hit) details = await tmdbDetails(tmdbApiKey, hit.id);
          } catch (err) {
            console.log(
              `[rottentomatoes/browse] tmdb error for "${query}": ${
                (err as Error).message
              }`,
            );
          }

          return {
            tmdbId: details?.id ?? null,
            rtSlug: it.slug,
            rtUrl: it.url,
            rtTitle: it.title,
            rtYear: it.year,
            tomatoScore: it.tomatoScore,
            popcornScore: it.popcornScore,
            browseSort: args.sort,
            tmdbName: details?.name ?? null,
            tmdbOriginalName: details?.original_name ?? null,
            firstAirDate: details?.first_air_date ?? null,
            status: details?.status ?? null,
            inProduction: details?.in_production ?? null,
            numberOfSeasons: details?.number_of_seasons ?? null,
            numberOfEpisodes: details?.number_of_episodes ?? null,
            networks: (details?.networks ?? []).map((n) => n.name),
            genres: (details?.genres ?? []).map((g) => g.name),
            overview: details?.overview ?? null,
            tmdbPopularity: details?.popularity ?? null,
            tmdbVoteAverage: details?.vote_average ?? null,
            tmdbVoteCount: details?.vote_count ?? null,
            homepage: details?.homepage ?? null,
            posterPath: details?.poster_path ?? null,
            collectedAt,
          };
        });

        const handles: unknown[] = [];
        let matched = 0;
        for (const show of enriched) {
          const safeSlug = show.rtSlug.replace(/[^A-Za-z0-9_-]/g, "_");
          const spec = show.tmdbId !== null
            ? String(show.tmdbId)
            : `rt-${safeSlug}`;
          if (show.tmdbId !== null) matched++;
          handles.push(await context.writeResource("show", spec, show));
        }
        console.log(
          `[rottentomatoes/browse] wrote ${handles.length} show resources (${matched} TMDB-matched, ${
            handles.length - matched
          } fallback)`,
        );

        return { dataHandles: handles };
      },
    },
  },
};
