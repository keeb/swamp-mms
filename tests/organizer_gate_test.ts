import { assert, assertEquals } from "jsr:@std/assert@1";
import {
  gateContext,
  listCatalogDirs,
  showDirOf,
  similarCatalogDirs,
} from "../extensions/models/mms_organizer.ts";

async function tempCatalog(): Promise<string> {
  const root = await Deno.makeTempDir();
  const A = `${root}/video/anime/completed`;
  for (
    const d of [
      `${A}/tokyo-revengers/s1`,
      `${A}/tokyo-revengers/s2`,
      `${A}/cyberpunk-edgerunners/s1`,
      `${A}/magical-explorer`,
      `${root}/video/shows/lioness/s2`,
      `${root}/video/shows/survivor/s51`,
    ]
  ) await Deno.mkdir(d, { recursive: true });
  return root;
}

Deno.test("showDirOf strips one trailing season dir", () => {
  assertEquals(showDirOf("/m/video/shows/x/s12"), "/m/video/shows/x");
  assertEquals(
    showDirOf("/m/video/anime/completed/x/"),
    "/m/video/anime/completed/x",
  );
});

Deno.test("similarCatalogDirs: arc title finds the franchise dir first", async () => {
  const root = await tempCatalog();
  const dirs = await listCatalogDirs(root);
  const top = similarCatalogDirs("Tokyo Revengers Santen Sensou-hen", dirs)[0];
  assertEquals(top.dir, `${root}/video/anime/completed/tokyo-revengers`);
  assertEquals(top.score, 1);
});

Deno.test("gateContext: new dir next to the same show is a split risk", async () => {
  const root = await tempCatalog();
  const dirs = await listCatalogDirs(root);
  const A = `${root}/video/anime/completed`;
  const g = await gateContext(
    `${A}/tokyo-revengers-santen-sensou-hen/s4`,
    dirs,
  );
  assertEquals(g.showDirExists, false);
  assert(g.splitRisk);
  assertEquals(g.similarDirs[0], `${A}/tokyo-revengers`);

  const c = await gateContext(`${A}/cyberpunk/s2`, dirs);
  assert(c.splitRisk, "bare 'cyberpunk' next to cyberpunk-edgerunners");
});

Deno.test("gateContext: existing dir or a genuinely new show is not a split risk", async () => {
  const root = await tempCatalog();
  const dirs = await listCatalogDirs(root);
  const existing = await gateContext(`${root}/video/shows/survivor/s52`, dirs);
  assertEquals(existing.showDirExists, true);
  assertEquals(existing.existingSeasons, ["s51"]);
  assertEquals(existing.splitRisk, false);

  const fresh = await gateContext(
    `${root}/video/anime/completed/chii-fuyo`,
    dirs,
  );
  assertEquals(fresh.splitRisk, false);

  // Filler words aren't a match at all.
  assertEquals(
    similarCatalogDirs("interview-with-the-vampire", [
      `${root}/video/shows/trafficked-with-mariana-van-zeller`,
    ]),
    [],
  );

  // Sharing one token isn't the same show.
  const other = await gateContext(
    `${root}/video/anime/completed/magical-girl-raising-project`,
    dirs,
  );
  assertEquals(other.splitRisk, false);
});
