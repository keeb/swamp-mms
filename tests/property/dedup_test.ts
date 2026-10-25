// Property-based tests for @keeb/mms/dedup pure helpers.
//
// Arbitraries are derived from the model's own zod schemas via
// @traversable/zod-test (the zod-4 successor to zod-fast-check), so every
// generated input is by construction "valid" according to the validator the
// model itself declares. The properties then assert the surrounding code
// actually handles the full valid input space.
import * as fc from "npm:fast-check@4.6.0";
import "npm:@traversable/zod-types@0.0.30";
import "npm:@traversable/registry@0.0.49";
import { zxTest } from "npm:@traversable/zod-test@0.0.28";
import { assert, assertEquals } from "jsr:@std/assert@1";
import {
  EpisodeSchema,
  fileExistsInDest,
  fileExistsInIndex,
  normalize,
  pickPreferred,
  slugDir,
} from "../../extensions/models/mms_dedup.ts";

const noopLogger = { info: (_: string) => {} };

const episodeArb = zxTest.fuzz(EpisodeSchema);

const fileIndexArb = fc.array(
  fc.string().map((name) => ({
    name,
    normalized: normalize(name),
    path: `/media/${name}`,
  })),
  { maxLength: 20 },
);

Deno.test("normalize: idempotent, lowercase alphanumeric only", () => {
  fc.assert(
    fc.property(fc.string({ unit: "binary" }), (s) => {
      const once = normalize(s);
      assertEquals(normalize(once), once);
      assert(/^[a-z0-9]*$/.test(once));
      assertEquals(normalize(s.toUpperCase()), once);
    }),
  );
});

Deno.test("slugDir: idempotent, no leading/trailing/double dashes", () => {
  fc.assert(
    fc.property(fc.string({ unit: "binary" }), (s) => {
      const once = slugDir(s);
      assertEquals(slugDir(once), once);
      assert(/^([a-z0-9]+(-[a-z0-9]+)*)?$/.test(once));
    }),
  );
});

Deno.test("fileExistsInIndex: total over schema-valid episodes", () => {
  // EpisodeSchema says `episode: z.string()` — ANY string is valid input,
  // so the helper must not throw on any of them.
  fc.assert(
    fc.property(episodeArb, fileIndexArb, (ep, index) => {
      const result = fileExistsInIndex(ep.show, ep.episode, index, noopLogger);
      assertEquals(typeof result, "boolean");
    }),
  );
});

Deno.test("fileExistsInIndex: finds canonical SubsPlease filenames", () => {
  const realisticShow = fc
    .stringMatching(/^[A-Za-z][A-Za-z0-9 ]{0,30}[A-Za-z0-9]$/)
    .filter((s) => normalize(s).length > 0);
  const realisticEp = fc.integer({ min: 0, max: 999 }).map(String);

  fc.assert(
    fc.property(realisticShow, realisticEp, (show, episode) => {
      const epPadded = episode.replace(/^0+/, "").padStart(2, "0");
      const name = `[SubsPlease] ${show} - ${epPadded} (1080p) [ABCD1234].mkv`;
      const index = [{ name, normalized: normalize(name), path: `/a/${name}` }];
      assert(fileExistsInIndex(show, episode, index, noopLogger));
    }),
  );
});

Deno.test("fileExistsInIndex: never matches an empty index", () => {
  fc.assert(
    fc.property(episodeArb, (ep) => {
      assertEquals(
        fileExistsInIndex(ep.show, ep.episode, [], noopLogger),
        false,
      );
    }),
  );
});

Deno.test("fileExistsInDest: total over schema-valid episodes", () => {
  const destPathArb = fc
    .array(fc.string({ minLength: 1 }).map(slugDir), {
      minLength: 1,
      maxLength: 3,
    })
    .map((parts) => "/" + parts.filter(Boolean).join("/"));
  fc.assert(
    fc.property(episodeArb, destPathArb, fileIndexArb, (ep, dest, index) => {
      const result = fileExistsInDest(
        dest,
        ep.season,
        ep.episode,
        index,
        noopLogger,
      );
      assertEquals(typeof result, "boolean");
    }),
  );
});

Deno.test("fileExistsInDest: finds organized SxxExx files under the slug dir", () => {
  const slugArb = fc
    .stringMatching(/^[a-z0-9]+(-[a-z0-9]+){0,3}$/)
    .filter((s) => s.length > 0 && slugDir(s) === s);
  const seasonArb = fc.integer({ min: 1, max: 40 }).map(String);
  const epArb = fc.integer({ min: 1, max: 99 }).map(String);

  fc.assert(
    fc.property(slugArb, seasonArb, epArb, (slug, season, episode) => {
      const sPad = String(parseInt(season)).padStart(2, "0");
      const ePad = episode.replace(/^0+/, "").padStart(2, "0");
      const name = `${slug}.S${sPad}E${ePad}.1080p.WEB.x265.mkv`;
      const index = [{
        name,
        normalized: normalize(name),
        path: `/media/tv/${slug}/s${parseInt(season)}/${name}`,
      }];
      const dest = `/media/tv/${slug}/s${parseInt(season)}`;
      assert(fileExistsInDest(dest, season, episode, index, noopLogger));
    }),
  );
});

Deno.test("fileExistsInDest strictSeason: another season's same episode never counts", () => {
  const slugArb = fc
    .stringMatching(/^[a-z0-9]+(-[a-z0-9]+){0,3}$/)
    .filter((s) => s.length > 0 && slugDir(s) === s);
  const seasonArb = fc.integer({ min: 1, max: 40 });
  const epArb = fc.integer({ min: 1, max: 99 });

  fc.assert(
    fc.property(
      slugArb,
      seasonArb,
      seasonArb,
      epArb,
      (slug, want, have, ep) => {
        fc.pre(want !== have);
        const e = String(ep).padStart(2, "0");
        const name = `${slug}.S${
          String(have).padStart(2, "0")
        }E${e}.1080p.WEB.mkv`;
        const index = [{
          name,
          normalized: normalize(name),
          path: `/media/tv/${slug}/s${have}/${name}`,
        }];
        const dest = `/media/tv/${slug}/s${want}`;
        assert(
          !fileExistsInDest(dest, String(want), String(ep), index, noopLogger, {
            strictSeason: true,
          }),
        );
      },
    ),
  );
});

Deno.test("fileExistsInDest strictSeason: AHS S13E07 is not S01E07; bare numbering needs the right season dir", () => {
  const file = (path: string) => {
    const name = path.slice(path.lastIndexOf("/") + 1);
    return { name, normalized: normalize(name), path };
  };
  const s1 = file(
    "/v/shows/american-horror-story/s1/American.Horror.Story.S01E07.mkv",
  );
  const dest = "/v/shows/american-horror-story/s13";
  // the old behavior, kept when the season is a guess
  assert(fileExistsInDest(dest, "13", "7", [s1], noopLogger));
  assert(
    !fileExistsInDest(dest, "13", "7", [s1], noopLogger, {
      strictSeason: true,
    }),
  );
  const s13 = file(
    "/v/shows/american-horror-story/s13/American.Horror.Story.S13E07.mkv",
  );
  assert(
    fileExistsInDest(dest, "13", "7", [s1, s13], noopLogger, {
      strictSeason: true,
    }),
  );
  const bareWrong = file("/v/anime/show/s2/[Grp] Show - 07 (1080p).mkv");
  const bareRight = file("/v/anime/show/s3/[Grp] Show - 07 (1080p).mkv");
  assert(
    !fileExistsInDest("/v/anime/show/s3", "3", "7", [bareWrong], noopLogger, {
      strictSeason: true,
    }),
  );
  assert(
    fileExistsInDest("/v/anime/show/s3", "3", "7", [bareRight], noopLogger, {
      strictSeason: true,
    }),
  );
});

Deno.test("pickPreferred: subset, unique per key, MeGusta wins", () => {
  const handleArb = zxTest.fuzz(EpisodeSchema).map((ep) => ({
    _show: ep.show,
    _episode: ep.episode,
    _season: ep.season,
    _rawTitle: ep.rawTitle,
    _data: ep,
  }));

  fc.assert(
    fc.property(fc.array(handleArb, { maxLength: 30 }), (handles) => {
      const picked = pickPreferred(handles);
      // Output is a subset of the input.
      assert(picked.length <= handles.length);
      for (const p of picked) assert(handles.includes(p));
      // Exactly one winner per show+season+episode key.
      const keys = picked.map((h) =>
        slugDir(`${h._show}-s${h._season ?? "0"}-e${h._episode}`)
      );
      assertEquals(new Set(keys).size, keys.length);
      // Every input key is represented.
      const inputKeys = new Set(
        handles.map((h) =>
          slugDir(`${h._show}-s${h._season ?? "0"}-e${h._episode}`)
        ),
      );
      assertEquals(new Set(keys).size, inputKeys.size);
    }),
  );

  // A MeGusta release always beats a plain release for the same episode.
  fc.assert(
    fc.property(episodeArb, (ep) => {
      const plain = {
        _show: ep.show,
        _episode: ep.episode,
        _season: ep.season,
        _rawTitle: "Some.Show.720p-Plain",
      };
      const megusta = {
        _show: ep.show,
        _episode: ep.episode,
        _season: ep.season,
        _rawTitle: "Some.Show.1080p.HEVC-MeGusta",
      };
      assertEquals(pickPreferred([plain, megusta]), [megusta]);
      assertEquals(pickPreferred([megusta, plain]), [megusta]);
    }),
  );
});
