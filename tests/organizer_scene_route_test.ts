import { assertEquals } from "jsr:@std/assert@1";
import {
  listCatalogDirs,
  type SceneParse,
  sceneRoute,
} from "../extensions/models/mms_organizer.ts";

async function catalog(): Promise<string> {
  const root = await Deno.makeTempDir();
  for (
    const d of [
      "video/shows/lioness/s2",
      "video/shows/survivor/s51",
      "video/shows/survivor-australia/s11",
      "video/shows/the-office/s1",
      "video/anime/completed/yozakura-san-chi-no-daisakusen/s2",
      "video/shows/dupe",
      "video/anime/completed/dupe",
      "video/movies",
    ]
  ) await Deno.mkdir(`${root}/${d}`, { recursive: true });
  return root;
}

const ep = (over: Partial<SceneParse>): SceneParse => ({
  style: "scene",
  kind: "episode",
  title: "Survivor",
  country: null,
  year: null,
  season: 51,
  episodes: [6],
  confidence: "high",
  ...over,
});

Deno.test("scene route: existing show, trailing year optional", async () => {
  const root = await catalog();
  const dirs = await listCatalogDirs(root);
  assertEquals(
    sceneRoute(ep({}), dirs, root)?.destPath,
    `${root}/video/shows/survivor/s51`,
  );
  assertEquals(
    sceneRoute(ep({ title: "Lioness", year: 2023, season: 3 }), dirs, root)
      ?.destPath,
    `${root}/video/shows/lioness/s3`,
  );
  const anime = sceneRoute(
    ep({ title: "Yozakura-san Chi no Daisakusen", season: 2 }),
    dirs,
    root,
  );
  assertEquals(anime?.mediaType, "anime");
});

Deno.test("scene route: a country code is never dropped", async () => {
  const root = await catalog();
  const dirs = await listCatalogDirs(root);
  // Survivor.AU must not land in survivor/ (US); survivor-au doesn't exist
  assertEquals(sceneRoute(ep({ country: "AU", season: 12 }), dirs, root), null);
  assertEquals(
    sceneRoute(
      ep({ title: "The Office", country: "US", season: 1 }),
      dirs,
      root,
    ),
    null,
  );
});

Deno.test("scene route: defers when unsure", async () => {
  const root = await catalog();
  const dirs = await listCatalogDirs(root);
  assertEquals(sceneRoute(ep({ confidence: "medium" }), dirs, root), null);
  assertEquals(
    sceneRoute(ep({ kind: "special", season: 0 }), dirs, root),
    null,
  );
  assertEquals(sceneRoute(ep({ title: "Dupe", season: 1 }), dirs, root), null);
  assertEquals(sceneRoute(ep({ style: "fansub" }), dirs, root), null);
});

Deno.test("scene route: a new show gets shows/<slug>/sN", async () => {
  const root = await catalog();
  const dirs = await listCatalogDirs(root);
  const c = sceneRoute(ep({ title: "Carrie", season: 1 }), dirs, root);
  assertEquals(
    [c?.destPath, c?.mediaType],
    [`${root}/video/shows/carrie/s1`, "tv_show"],
  );
  // year dropped like lioness; filler words don't make it look like a known show
  assertEquals(
    sceneRoute(
      ep({ title: "Interview with the Vampire", year: 2022, season: 3 }),
      [...dirs, `${root}/video/shows/whats-next-the-future-with-bill-gates`],
      root,
    )?.destPath,
    `${root}/video/shows/interview-with-the-vampire/s3`,
  );
  // anime / remap releases keep the LLM
  assertEquals(
    sceneRoute(ep({ title: "Carrie", season: 1 }), dirs, root, false),
    null,
  );
});

Deno.test("scene route: a new show that looks like a known one defers", async () => {
  const root = await catalog();
  const dirs = await listCatalogDirs(root);
  // a prefix of / extension of an existing dir
  assertEquals(
    sceneRoute(ep({ title: "Lioness Origins", season: 1 }), dirs, root),
    null,
  );
  // every word of an existing dir's name
  assertEquals(
    sceneRoute(ep({ title: "The Lioness", season: 1 }), dirs, root),
    null,
  );
  assertEquals(
    sceneRoute(ep({ title: "Office Ladies", season: 1 }), dirs, root),
    null,
  );
});

Deno.test("scene route: features with a year go to movies", async () => {
  const root = await catalog();
  const dirs = await listCatalogDirs(root);
  const f = sceneRoute(
    ep({
      kind: "feature",
      title: "Prisoners",
      year: 2013,
      season: null,
      episodes: [],
    }),
    dirs,
    root,
  );
  assertEquals([f?.destPath, f?.mediaType], [`${root}/video/movies`, "movie"]);
  assertEquals(
    sceneRoute(
      ep({ kind: "feature", title: "X", year: null, season: null }),
      dirs,
      root,
    ),
    null,
  );
});
