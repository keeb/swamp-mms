import { z } from "npm:zod@4";

const GlobalArgsSchema = z.object({
  apiKey: z.string().meta({ sensitive: true }).describe("TMDB API key (v3)"),
});

const TvShowSchema = z.object({
  tmdbId: z.number(),
  name: z.string(),
  originalName: z.string().optional(),
  overview: z.string().nullable().optional(),
  status: z.string().optional(),
  type: z.string().optional(),
  firstAirDate: z.string().nullable().optional(),
  lastAirDate: z.string().nullable().optional(),
  numberOfSeasons: z.number().nullable().optional(),
  numberOfEpisodes: z.number().nullable().optional(),
  inProduction: z.boolean().optional(),
  nextEpisodeToAir: z.string().nullable().optional(),
  voteAverage: z.number().nullable().optional(),
  popularity: z.number().nullable().optional(),
  genres: z.array(z.string()).optional(),
  networks: z.array(z.string()).optional(),
  originCountry: z.array(z.string()).optional(),
  url: z.string().optional(),
});

const MovieSchema = z.object({
  tmdbId: z.number(),
  title: z.string(),
  originalTitle: z.string().optional(),
  overview: z.string().nullable().optional(),
  status: z.string().optional(),
  releaseDate: z.string().nullable().optional(),
  runtime: z.number().nullable().optional(),
  budget: z.number().nullable().optional(),
  revenue: z.number().nullable().optional(),
  voteAverage: z.number().nullable().optional(),
  voteCount: z.number().nullable().optional(),
  popularity: z.number().nullable().optional(),
  tagline: z.string().nullable().optional(),
  genres: z.array(z.string()).optional(),
  productionCompanies: z.array(z.string()).optional(),
  productionCountries: z.array(z.string()).optional(),
  spokenLanguages: z.array(z.string()).optional(),
  originalLanguage: z.string().optional(),
  homepage: z.string().nullable().optional(),
  imdbId: z.string().nullable().optional(),
  url: z.string().optional(),
});

const BASE = "https://api.themoviedb.org/3";

async function tmdbFetch(
  path: string,
  apiKey: string,
  params?: Record<string, string>,
) {
  const url = new URL(`${BASE}${path}`);
  url.searchParams.set("api_key", apiKey);
  if (params) {
    for (const [k, v] of Object.entries(params)) {
      url.searchParams.set(k, v);
    }
  }
  const res = await fetch(url.toString());
  if (!res.ok) {
    throw new Error(`TMDB API error: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

function slugify(s: string): string {
  return s
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "")
    .slice(0, 80);
}

export const model = {
  type: "@keeb/tmdb-lookup",
  version: "2026.07.03.1",
  globalArguments: GlobalArgsSchema,
  upgrades: [
    {
      toVersion: "2026.05.14.1",
      description:
        "Add searchMovie method and movie resource; no globalArgs change",
      upgradeAttributes: (old) => old,
    },
  ],
  resources: {
    show: {
      description: "TV show info from TMDB",
      schema: TvShowSchema,
      lifetime: "infinite" as const,
      garbageCollection: 50,
    },
    movie: {
      description: "Movie info from TMDB",
      schema: MovieSchema,
      lifetime: "infinite" as const,
      garbageCollection: 50,
    },
  },
  methods: {
    search: {
      description:
        "Search TMDB for a TV show by name, fetches full details including series status",
      arguments: z.object({
        query: z.string().describe("TV show name to search for"),
        limit: z.number().default(5).describe("Max results to return"),
      }),
      execute: async (args, context) => {
        const { apiKey } = context.globalArgs;
        const { logger } = context;

        const searchData = await tmdbFetch("/search/tv", apiKey, {
          query: args.query,
        });

        const results = (searchData.results || []).slice(0, args.limit);
        logger.info(`Found ${results.length} results for "${args.query}"`);

        const handles = [];

        for (const result of results) {
          const details = await tmdbFetch(`/tv/${result.id}`, apiKey);

          const handle = await context.writeResource(
            "show",
            slugify(details.name || result.name),
            {
              tmdbId: details.id,
              name: details.name,
              originalName: details.original_name || undefined,
              overview: details.overview || null,
              status: details.status || undefined,
              type: details.type || undefined,
              firstAirDate: details.first_air_date || null,
              lastAirDate: details.last_air_date || null,
              numberOfSeasons: details.number_of_seasons ?? null,
              numberOfEpisodes: details.number_of_episodes ?? null,
              inProduction: details.in_production ?? undefined,
              nextEpisodeToAir: details.next_episode_to_air?.air_date ?? null,
              voteAverage: details.vote_average ?? null,
              popularity: details.popularity ?? null,
              // deno-lint-ignore no-explicit-any
              genres: (details.genres || []).map((g: any) => g.name),
              // deno-lint-ignore no-explicit-any
              networks: (details.networks || []).map((n: any) => n.name),
              originCountry: details.origin_country || undefined,
              url: `https://www.themoviedb.org/tv/${details.id}`,
            },
          );
          handles.push(handle);
        }

        return { dataHandles: handles };
      },
    },
    seasons: {
      description:
        "List a TV show's seasons with cumulative absolute episode ranges — for computing episode→season breaks",
      arguments: z.object({
        tmdbId: z.number().describe("TMDB TV id"),
      }),
      execute: async (args, context) => {
        const { apiKey } = context.globalArgs;
        const details = await tmdbFetch(`/tv/${args.tmdbId}`, apiKey);
        context.logger.info(`Show: ${details.name} (id ${details.id})`);
        let cum = 0;
        // deno-lint-ignore no-explicit-any
        for (const s of (details.seasons || []) as any[]) {
          if (s.season_number === 0) continue; // specials
          const start = cum + 1;
          cum += s.episode_count;
          context.logger.info(
            `s${s.season_number}\t${start}-${cum}\t(${s.episode_count})\t${s.name}`,
          );
        }
        return { dataHandles: [] };
      },
    },
    searchMovie: {
      description:
        "Search TMDB for a movie by title, fetches full details (release date, runtime, vote, etc.)",
      arguments: z.object({
        query: z.string().describe("Movie title to search for"),
        year: z.number().optional().describe(
          "Optional release-year hint to disambiguate",
        ),
        limit: z.number().default(5).describe("Max results to return"),
      }),
      execute: async (args, context) => {
        const { apiKey } = context.globalArgs;
        const { logger } = context;

        const params: Record<string, string> = { query: args.query };
        if (args.year) params.year = String(args.year);
        const searchData = await tmdbFetch("/search/movie", apiKey, params);

        const results = (searchData.results || []).slice(0, args.limit);
        logger.info(
          `Found ${results.length} movie results for "${args.query}"`,
        );

        const handles = [];
        for (const result of results) {
          const details = await tmdbFetch(`/movie/${result.id}`, apiKey);

          const handle = await context.writeResource(
            "movie",
            slugify(details.title || result.title),
            {
              tmdbId: details.id,
              title: details.title,
              originalTitle: details.original_title || undefined,
              overview: details.overview || null,
              status: details.status || undefined,
              releaseDate: details.release_date || null,
              runtime: details.runtime ?? null,
              budget: details.budget ?? null,
              revenue: details.revenue ?? null,
              voteAverage: details.vote_average ?? null,
              voteCount: details.vote_count ?? null,
              popularity: details.popularity ?? null,
              tagline: details.tagline || null,
              // deno-lint-ignore no-explicit-any
              genres: (details.genres || []).map((g: any) => g.name),
              productionCompanies: (details.production_companies || []).map(
                // deno-lint-ignore no-explicit-any
                (c: any) => c.name,
              ),
              productionCountries: (details.production_countries || []).map(
                // deno-lint-ignore no-explicit-any
                (c: any) => c.name,
              ),
              spokenLanguages: (details.spoken_languages || []).map(
                // deno-lint-ignore no-explicit-any
                (l: any) => l.english_name || l.name,
              ),
              originalLanguage: details.original_language || undefined,
              homepage: details.homepage || null,
              imdbId: details.imdb_id || null,
              url: `https://www.themoviedb.org/movie/${details.id}`,
            },
          );
          handles.push(handle);
        }

        return { dataHandles: handles };
      },
    },
  },
};
