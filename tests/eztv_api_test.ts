import { assertEquals } from "jsr:@std/assert@1";
import {
  episodeKey,
  episodeKeyFromTitle,
  eztvApiItem,
} from "../extensions/models/mms_source.ts";

Deno.test("eztvApiItem: filename title, season/episode, magnet", () => {
  const item = eztvApiItem({
    title: "Survivor S51E05 1080p HEVC x265-MeGusta EZTV",
    filename: "Survivor.S51E05.1080p.HEVC.x265-MeGusta[EZTVx.to].mkv",
    season: "51",
    episode: "5",
    hash: "4c85bc",
    magnet_url: "magnet:?xt=urn:btih:4c85bc",
    seeds: 878,
    size_bytes: "2113115319",
    date_released_unix: 1792644977,
  });
  assertEquals(item._api, true);
  assertEquals(item.title, "Survivor.S51E05.1080p.HEVC.x265-MeGusta[EZTVx.to]");
  assertEquals(item._season, "51");
  assertEquals(item._episode, "5");
  assertEquals(item._size, 2113115319);
  assertEquals(item["torrent:magnetURI"], "magnet:?xt=urn:btih:4c85bc");
});

Deno.test("eztvApiItem: season pack (episode 0) carries no episode", () => {
  const item = eztvApiItem({
    title: "Survivor S50 1080p",
    season: "50",
    episode: "0",
  });
  assertEquals(item._season, undefined);
  assertEquals(item._episode, undefined);
});

Deno.test("episodeKey: numeric normalization, from fields or title", () => {
  assertEquals(episodeKey("51", "05"), "S51E5");
  assertEquals(episodeKey(51, 5), "S51E5");
  assertEquals(episodeKey("51", "0"), null);
  assertEquals(episodeKey(undefined, "5"), null);
  assertEquals(episodeKeyFromTitle("Survivor.S51E05.1080p.WEB"), "S51E5");
  assertEquals(episodeKeyFromTitle("Survivor.S51.1080p"), null);
});
