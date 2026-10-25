import { assert, assertEquals } from "jsr:@std/assert@1";
import {
  explicitSeason,
  fileExistsInDest,
  fileExistsInIndex,
  normalize,
  pickPreferred,
  splitVersion,
} from "../extensions/models/mms_dedup.ts";

const logger = { info: () => {} };

function index(paths: string[]) {
  return paths.map((path) => {
    const name = path.substring(path.lastIndexOf("/") + 1);
    return { name, normalized: normalize(name), path };
  });
}

Deno.test("splitVersion strips vN suffixes", () => {
  assertEquals(splitVersion("01v2"), { base: "01", version: 2 });
  assertEquals(splitVersion("09 V2"), { base: "09", version: 2 });
  assertEquals(splitVersion("49"), { base: "49", version: 1 });
  assertEquals(splitVersion("1152"), { base: "1152", version: 1 });
});

Deno.test("explicitSeason only reads SxxEyy from the title", () => {
  assertEquals(
    explicitSeason("[DKB] Tokyo Revengers: Santen Sensou-hen - S04E04 [1080p]"),
    "4",
  );
  assertEquals(
    explicitSeason("[Erai-raws] Tokyo Revengers - 04 [1080p]"),
    null,
  );
  assertEquals(explicitSeason(undefined), null);
});

Deno.test("subsplease v2 re-release counts as on disk when v1 is", () => {
  const files = index([
    "/a/yasei-no-last-boss-ga-arawareta/[EMBER] Yasei no Last Boss ga Arawareta! - 01 V2.mkv",
    "/a/yasei-no-last-boss-ga-arawareta/[EMBER] Yasei no Last Boss ga Arawareta! - 03.mkv",
  ]);
  assert(
    fileExistsInIndex(
      "Yasei no Last Boss ga Arawareta!",
      "01v2",
      files,
      logger,
    ),
  );
  assert(
    fileExistsInIndex(
      "Yasei no Last Boss ga Arawareta!",
      "03v2",
      files,
      logger,
    ),
  );
  assert(
    !fileExistsInIndex("Yasei no Last Boss ga Arawareta!", "13", files, logger),
  );
});

Deno.test("pickPreferred collapses 49 and 49v2, keeping v2", () => {
  const picked = pickPreferred([
    { _show: "Kusuriya no Hitorigoto", _episode: "49", _rawTitle: "a" },
    { _show: "Kusuriya no Hitorigoto", _episode: "49v2", _rawTitle: "b" },
    { _show: "Kusuriya no Hitorigoto", _episode: "50", _rawTitle: "c" },
  ]);
  assertEquals(picked.map((p) => p._episode).sort(), ["49v2", "50"]);
});

Deno.test("strict season: S04E04 is not satisfied by S01E04 or a season-2 dir", () => {
  const files = index([
    "/a/tokyo-revengers/s1/S01E04-Return [D7BB8FC7].mkv",
    "/a/tokyo-revengers/s2/[Anime Time] Tokyo Revengers Season 2 - 04.mkv",
  ]);
  assert(
    !fileExistsInDest("/tokyo-revengers/s4", "4", "04", files, logger, {
      strictSeason: true,
    }),
  );
  // Lenient (LLM-only season) keeps the old skip-leaning behaviour.
  assert(fileExistsInDest("/tokyo-revengers/s4", "4", "04", files, logger));
});

Deno.test("strict season still finds the episode when it is on disk", () => {
  const files = index([
    "/a/yozakura-san-chi-no-daisakusen/s2/[DKB] Yozakura-san Chi no Daisakusen - S02E12 [1080p][HEVC x265 10bit][1FCEF4CA].mkv",
  ]);
  assert(
    fileExistsInDest(
      "/yozakura-san-chi-no-daisakusen/s2",
      "2",
      "12",
      files,
      logger,
      {
        strictSeason: true,
      },
    ),
  );
  assert(
    !fileExistsInDest(
      "/yozakura-san-chi-no-daisakusen/s2",
      "2",
      "13",
      files,
      logger,
      {
        strictSeason: true,
      },
    ),
  );
});
