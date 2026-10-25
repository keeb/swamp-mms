import { assertEquals } from "jsr:@std/assert@1";
import {
  sanitizeDestPath,
  specialCase,
} from "../extensions/models/mms_organizer.ts";

const ROOT = "/home/keeb/media";
const anime = (p: string) => `${ROOT}/video/anime/completed/${p}`;
const dest = (f: string) => specialCase(f, ROOT)?.destPath ?? null;

Deno.test("special case: Steel Ball Run is flat despite DKB S06 numbering", () => {
  for (const e of ["01", "02", "06"]) {
    assertEquals(
      dest(
        `[DKB] Steel Ball Run JoJo no Kimyou na Bouken - S06E${e} [1080p][HEVC x265 10bit][Dual-Audio][Multi-Subs][7F9F9E8E].mkv`,
      ),
      anime("steel-ball-run-jojo-no-kimyou-na-bouken"),
    );
  }
});

Deno.test("special case: Tokyo Revengers Santen Sensou-hen is tokyo-revengers/s4", () => {
  assertEquals(
    dest(
      "[DKB] Tokyo Revengers Santen Sensou-hen - S04E03 [1080p][HEVC x265 10bit][Dual-Audio][Multi-Subs][979D1434].mkv",
    ),
    anime("tokyo-revengers/s4"),
  );
});

Deno.test("special case: Kusuriya continuous numbering splits into s1/s2/s3, v2 included", () => {
  const k = (ep: string) =>
    dest(`[SubsPlease] Kusuriya no Hitorigoto - ${ep} (1080p) [F14E89B9].mkv`);
  assertEquals(k("01"), anime("kusuriya-no-hitorigoto/s1"));
  assertEquals(k("24"), anime("kusuriya-no-hitorigoto/s1"));
  assertEquals(k("25"), anime("kusuriya-no-hitorigoto/s2"));
  assertEquals(k("48"), anime("kusuriya-no-hitorigoto/s2"));
  assertEquals(k("49v2"), anime("kusuriya-no-hitorigoto/s3"));
  assertEquals(k("52"), anime("kusuriya-no-hitorigoto/s3"));
});

Deno.test("special case: Cyberpunk / Cyberpunk Edgerunners share one dir", () => {
  assertEquals(
    dest("[Judas] Cyberpunk - S02E01.mkv"),
    anime("cyberpunk-edgerunners/s2"),
  );
  assertEquals(
    dest("[Judas] Cyberpunk Edgerunners - S01E10.mkv"),
    anime("cyberpunk-edgerunners/s1"),
  );
});

Deno.test("special case: Hotel Inhumans season 1 has no marker", () => {
  assertEquals(
    dest("[SubsPlease] Hotel Inhumans - 05 (1080p) [E6B24346].mkv"),
    anime("hotel-inhumans/s1"),
  );
  assertEquals(
    dest("[SubsPlease] Hotel Inhumans S2 - 02 (1080p) [A21A5991].mkv"),
    anime("hotel-inhumans/s2"),
  );
});

Deno.test("special case: both Lioness release names land in shows/lioness", () => {
  assertEquals(
    dest("Special.Ops.Lioness.S01E03.1080p.x265-ELiTE[EZTVx.to].mkv"),
    `${ROOT}/video/shows/lioness/s1`,
  );
  assertEquals(
    dest("Lioness.2023.S03E07.1080p.HEVC.x265-MeGusta[EZTVx.to].mkv"),
    `${ROOT}/video/shows/lioness/s3`,
  );
});

Deno.test("special case: unrelated releases fall through", () => {
  assertEquals(
    dest("Survivor.S51E05.1080p.HEVC.x265-MeGusta[EZTVx.to].mkv"),
    null,
  );
  assertEquals(
    dest("[SubsPlease] Magical Explorer - 05 (1080p) [BE62F093].mkv"),
    null,
  );
});

Deno.test("sanitizeDestPath strips forbidden punctuation from LLM paths", () => {
  assertEquals(
    sanitizeDestPath(anime("seitokai-ni-mo-ana-wa-aru!/s1"), ROOT),
    anime("seitokai-ni-mo-ana-wa-aru/s1"),
  );
  assertEquals(sanitizeDestPath(anime("clevatess"), ROOT), anime("clevatess"));
});
