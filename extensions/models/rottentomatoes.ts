import { z } from "npm:zod@4";

// ── Schemas ─────────────────────────────────────────────────────────────────

const GlobalArgs = z.object({
  tmdbApiKey: z.string({ sensitive: true }).describe("TMDB API key (v3)"),
  headless: z.boolean().default(true).describe(
    "Run Playwright headless.",
  ),
  nodeModulesDir: z.string().optional().describe(
    "node_modules directory that already has playwright installed. When unset, the model installs a pinned playwright into $XDG_CACHE_HOME/swamp-mms/scraper (default ~/.cache) on first use, so no consumer-repo node_modules is needed.",
  ),
});

// Pinned to match the chromium build already installed under
// ~/.cache/ms-playwright; bump both together.
const PLAYWRIGHT_VERSION = "1.60.0";

const BrowseArgs = z.object({
  sort: z.enum(["newest", "popular"]).describe(
    "Which Rotten Tomatoes TV browse list to scrape",
  ),
  loadMoreClicks: z.number().int().min(0).default(0).describe(
    "How many times to click Load More before extracting (0 = first page only)",
  ),
});

const BrowseMoviesArgs = z.object({
  list: z.enum(["at_home", "in_theaters", "coming_soon"]).default("at_home")
    .describe(
      "Which Rotten Tomatoes movie shelf to scrape. at_home = streaming/VOD (watchable now), in_theaters = theatrical, coming_soon = unreleased",
    ),
  // RT honours sort:newest reliably on movie shelves. sort:popular and the
  // score sorts frequently come back alphabetised (page 1 is all "#..." titles)
  // — RT degrades them under bot mitigation, and driving the on-page sort
  // control does not fix it. Prefer newest; treat the others as best-effort.
  sort: z.enum(["popular", "newest", "critic_highest", "audience_highest"])
    .default("newest").describe(
      "Sort order within the shelf. 'newest' is the only one RT honours reliably — the others often degrade to an alphabetical listing.",
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

const MovieSchema = z.object({
  // Identity
  tmdbId: z.number().nullable().describe(
    "TMDB movie ID, null if no TMDB match was found",
  ),
  rtSlug: z.string().describe("Rotten Tomatoes slug, e.g. 'sinners_2025'"),
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
  browseList: z.enum(["at_home", "in_theaters", "coming_soon"]).describe(
    "Which RT movie shelf this was last seen on",
  ),
  browseSort: z.enum([
    "popular",
    "newest",
    "critic_highest",
    "audience_highest",
  ])
    .describe("Which sort order this was last seen under"),
  // TMDB enrichment (null if no match)
  tmdbTitle: z.string().nullable(),
  tmdbOriginalTitle: z.string().nullable(),
  releaseDate: z.string().nullable(),
  status: z.string().nullable(),
  runtime: z.number().nullable().describe("Runtime in minutes"),
  studios: z.array(z.string()).default([]).describe(
    "TMDB production companies — the movie analogue of a show's networks",
  ),
  genres: z.array(z.string()).default([]),
  overview: z.string().nullable(),
  tagline: z.string().nullable(),
  tmdbPopularity: z.number().nullable(),
  tmdbVoteAverage: z.number().nullable(),
  tmdbVoteCount: z.number().nullable(),
  homepage: z.string().nullable(),
  posterPath: z.string().nullable(),
  collectedAt: z.string().describe("ISO timestamp of this scrape"),
});

// ── Helpers ─────────────────────────────────────────────────────────────────

// Resolve the node_modules the scraper loads playwright from: the configured
// directory, or a cache-dir install of the pinned version (installed once).
async function scraperNodeModules(configured?: string): Promise<string> {
  if (configured) return configured;
  const cacheRoot = Deno.env.get("XDG_CACHE_HOME") ??
    `${Deno.env.get("HOME")}/.cache`;
  const prefix = `${cacheRoot}/swamp-mms/scraper`;
  const nodeModules = `${prefix}/node_modules`;
  try {
    const pkg = JSON.parse(
      await Deno.readTextFile(`${nodeModules}/playwright/package.json`),
    );
    if (pkg.version === PLAYWRIGHT_VERSION) return nodeModules;
  } catch {
    // not installed yet
  }
  console.log(
    `[rottentomatoes] installing playwright@${PLAYWRIGHT_VERSION} into ${prefix}`,
  );
  await Deno.mkdir(prefix, { recursive: true });
  const { code, stderr } = await new Deno.Command("npm", {
    args: [
      "install",
      "--prefix",
      prefix,
      "--no-save",
      "--no-audit",
      "--no-fund",
      `playwright@${PLAYWRIGHT_VERSION}`,
    ],
    stdout: "null",
    stderr: "piped",
  }).output();
  if (code !== 0) {
    throw new Error(
      `npm install playwright@${PLAYWRIGHT_VERSION} failed (exit ${code}): ${
        new TextDecoder().decode(stderr)
      }`,
    );
  }
  return nodeModules;
}

async function runScraper(
  scriptPath: string,
  nodeModules: string,
  method: string,
  headless: boolean,
  methodArgs: Record<string, unknown>,
) {
  const cmd = new Deno.Command("node", {
    args: [scriptPath, method, String(headless), JSON.stringify(methodArgs)],
    env: { NODE_PATH: nodeModules },
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

type TmdbMovieHit = { id: number };

type TmdbMovieDetails = {
  id: number;
  title: string;
  original_title: string;
  release_date?: string;
  status?: string;
  runtime?: number;
  production_companies?: { name: string }[];
  genres?: { name: string }[];
  overview?: string;
  tagline?: string;
  popularity?: number;
  vote_average?: number;
  vote_count?: number;
  homepage?: string;
  poster_path?: string;
};

// Movies live in a separate TMDB namespace from TV — different search endpoint,
// different detail endpoint, and different date parameter.
async function tmdbSearchMovie(
  apiKey: string,
  query: string,
  year: number | null,
): Promise<TmdbMovieHit | null> {
  const params = new URLSearchParams({ api_key: apiKey, query });
  if (year) params.set("primary_release_year", String(year));
  const resp = await fetch(
    `https://api.themoviedb.org/3/search/movie?${params}`,
  );
  if (!resp.ok) {
    console.log(
      `[rottentomatoes] tmdb movie search HTTP ${resp.status} for query="${query}" year=${year}`,
    );
    return null;
  }
  const json = await resp.json() as { results?: TmdbMovieHit[] };
  if (!json.results || json.results.length === 0) {
    if (year) return await tmdbSearchMovie(apiKey, query, null);
    return null;
  }
  return json.results[0];
}

async function tmdbMovieDetails(
  apiKey: string,
  id: number,
): Promise<TmdbMovieDetails | null> {
  const resp = await fetch(
    `https://api.themoviedb.org/3/movie/${id}?api_key=${apiKey}`,
  );
  if (!resp.ok) {
    console.log(
      `[rottentomatoes] tmdb movie details HTTP ${resp.status} for id=${id}`,
    );
    return null;
  }
  return await resp.json() as TmdbMovieDetails;
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
  version: "2026.08.02.1",
  resources: {
    show: {
      description:
        "A single TV show: Rotten Tomatoes scores + TMDB metadata. Spec is the TMDB ID (or rt-<slug> when no match).",
      schema: ShowSchema,
      lifetime: "infinite",
      garbageCollection: 30,
    },
    movie: {
      description:
        "A single movie: Rotten Tomatoes scores + TMDB metadata. Spec is the TMDB movie ID (or rt-<slug> when no match). Kept separate from `show` because TMDB movie and TV IDs share numeric values but name different titles.",
      schema: MovieSchema,
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
          await scraperNodeModules(context.globalArgs.nodeModulesDir),
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

    browse_movies: {
      description:
        "Scrape a Rotten Tomatoes movie browse shelf, enrich each movie via TMDB, and write one resource per movie keyed by TMDB movie ID. Default shelf is at_home (streaming/VOD).",
      arguments: BrowseMoviesArgs,
      execute: async (args, context) => {
        const { tmdbApiKey, headless } = context.globalArgs;
        console.log(
          `[rottentomatoes/browse_movies] list=${args.list} sort=${args.sort} loadMoreClicks=${args.loadMoreClicks}`,
        );

        const scriptPath = context.extensionFile(
          "scripts/rottentomatoes-scraper.ts",
        );
        const scraped = await runScraper(
          scriptPath,
          await scraperNodeModules(context.globalArgs.nodeModulesDir),
          "browse_movies",
          headless,
          {
            list: args.list,
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
          `[rottentomatoes/browse_movies] scraped ${items.length} tiles; enriching via TMDB`,
        );

        const enriched = await mapWithConcurrency(items, 4, async (it) => {
          const query = slugToQuery(it.slug) || it.title;
          let details: TmdbMovieDetails | null = null;
          try {
            const hit = await tmdbSearchMovie(tmdbApiKey, query, it.year);
            if (hit) details = await tmdbMovieDetails(tmdbApiKey, hit.id);
          } catch (err) {
            console.log(
              `[rottentomatoes/browse_movies] tmdb error for "${query}": ${
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
            browseList: args.list,
            browseSort: args.sort,
            tmdbTitle: details?.title ?? null,
            tmdbOriginalTitle: details?.original_title ?? null,
            releaseDate: details?.release_date ?? null,
            status: details?.status ?? null,
            runtime: details?.runtime ?? null,
            studios: (details?.production_companies ?? []).map((c) => c.name),
            genres: (details?.genres ?? []).map((g) => g.name),
            overview: details?.overview ?? null,
            tagline: details?.tagline ?? null,
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
        for (const movie of enriched) {
          const safeSlug = movie.rtSlug.replace(/[^A-Za-z0-9_-]/g, "_");
          const spec = movie.tmdbId !== null
            ? String(movie.tmdbId)
            : `rt-${safeSlug}`;
          if (movie.tmdbId !== null) matched++;
          handles.push(await context.writeResource("movie", spec, movie));
        }
        console.log(
          `[rottentomatoes/browse_movies] wrote ${handles.length} movie resources (${matched} TMDB-matched, ${
            handles.length - matched
          } fallback)`,
        );

        return { dataHandles: handles };
      },
    },
  },
};
