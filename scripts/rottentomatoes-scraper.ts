// rottentomatoes-scraper.ts — called by the swamp rottentomatoes model via Deno.Command.
// Invoked as: node rottentomatoes-scraper.ts <method> <headless> [argsJson]
//   argsJson: { sort: "newest" | "popular", loadMoreClicks?: number }
// Outputs JSON to stdout; progress/errors to stderr.
//
// Runs under Node (not Deno) because Playwright is the canonical browser
// automation lib here. Allowed extension types in swamp's additionalFiles list
// don't include .js, so we ship as .ts and let Node's native type-stripping run
// it (Node 22+). A package.json next to this file pins "type": "module" so the
// import below is parsed as ESM.

// Resolve playwright via CJS createRequire so NODE_PATH (set by the swamp model
// to the consumer repo's node_modules) is honored. ESM resolution does not
// consult NODE_PATH, which would break when this script ships inside the
// extension bundle rather than alongside a node_modules tree.
import { createRequire } from "node:module";
import process from "node:process";
const require = createRequire(import.meta.url);
const { chromium } = require("playwright");
// deno-lint-ignore no-explicit-any
type Page = any;

function randomDelay(minMs: number, maxMs: number): Promise<void> {
  const ms = Math.floor(Math.random() * (maxMs - minMs) + minMs);
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const SORT_URLS: Record<string, string> = {
  newest: "https://www.rottentomatoes.com/browse/tv_series_browse/sort:newest",
  popular:
    "https://www.rottentomatoes.com/browse/tv_series_browse/sort:popular",
};

async function clickLoadMoreIfPresent(page: Page): Promise<boolean> {
  return await page.evaluate(() => {
    const candidates = Array.from(document.querySelectorAll("button, a"));
    const btn = candidates.find((el) =>
      /load\s*more/i.test((el.textContent || "").trim())
    ) as HTMLElement | undefined;
    if (!btn) return false;
    btn.scrollIntoView({ block: "center" });
    btn.click();
    return true;
  });
}

async function extractTiles(page: Page) {
  return await page.evaluate(() => {
    const tiles = Array.from(
      document.querySelectorAll(
        "a[data-qa='discovery-media-list-item-caption'], a[href^='/tv/']",
      ),
    );
    const byHref = new Map<
      string,
      {
        title: string;
        year: number | null;
        slug: string;
        url: string;
        tomatoScore: number | null;
        popcornScore: number | null;
      }
    >();
    for (const a of tiles) {
      const href = (a.getAttribute("href") || "").replace(/\/$/, "");
      if (!/^\/tv\/[^/]+$/.test(href)) continue;
      if (byHref.has(href)) continue;

      const raw = (a.textContent || "").replace(/ /g, " ");
      const lines = raw.split(/\n+/).map((s) => s.trim()).filter(Boolean);
      const title = lines.find((line) => {
        if (/^\d{1,3}\s*%$/.test(line)) return false;
        if (/^Latest Episode\b/i.test(line)) return false;
        return true;
      });
      if (!title) continue;

      let scope: Element | null = a;
      for (let i = 0; i < 6 && scope; i++) {
        if (
          scope.querySelector &&
          scope.querySelector("score-pairs-deprecated, score-pairs")
        ) break;
        scope = scope.parentElement;
      }
      const scoreEl = scope && scope.querySelector
        ? scope.querySelector("score-pairs-deprecated, score-pairs")
        : null;

      const parseScore = (raw: string | null): number | null => {
        if (raw === null || raw === undefined || raw === "") return null;
        const s = String(raw).replace("%", "").trim();
        if (!s || s === "--") return null;
        const n = parseInt(s, 10);
        return Number.isFinite(n) ? n : null;
      };

      let tomatoScore: number | null = null;
      let popcornScore: number | null = null;
      if (scoreEl) {
        tomatoScore = parseScore(
          scoreEl.getAttribute("criticsscore") ||
            scoreEl.getAttribute("tomatometerscore"),
        );
        popcornScore = parseScore(scoreEl.getAttribute("audiencescore"));
      }

      if (tomatoScore === null && popcornScore === null && scope) {
        const txt = (scope.textContent || "").match(/(\d{1,3})\s*%/g) || [];
        const nums = txt.map((s) => parseInt(s, 10)).filter((n) =>
          n >= 0 && n <= 100
        );
        if (nums.length >= 1) tomatoScore = nums[0];
        if (nums.length >= 2) popcornScore = nums[1];
      }

      let year: number | null = null;
      const yearMatch = href.match(/_(\d{4})\/?$/);
      if (yearMatch) year = parseInt(yearMatch[1], 10);

      const slug = href.replace(/^\/tv\//, "").replace(/\/$/, "");
      const url = `https://www.rottentomatoes.com${href}`;

      byHref.set(href, { title, year, slug, url, tomatoScore, popcornScore });
    }
    return Array.from(byHref.values());
  });
}

async function browse(
  headless: string,
  args: { sort?: string; loadMoreClicks?: number },
) {
  const sort = args.sort || "newest";
  const url = SORT_URLS[sort];
  if (!url) {
    throw new Error(
      `Unknown sort: ${sort}. Use one of: ${Object.keys(SORT_URLS).join(", ")}`,
    );
  }

  const loadMoreClicks = args.loadMoreClicks ?? 0;

  const browser = await chromium.launch({
    headless: headless === "true" || headless === true as unknown as string,
  });
  const context = await browser.newContext({
    viewport: { width: 1280, height: 900 },
    userAgent:
      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
  });
  const page = await context.newPage();

  try {
    process.stderr.write(
      `[rt/browse] sort=${sort} loadMoreClicks=${loadMoreClicks}\n`,
    );
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });
    await page.waitForSelector("a[href^='/tv/']", {
      state: "attached",
      timeout: 30000,
    });
    await randomDelay(2500, 3500);

    for (let i = 0; i < loadMoreClicks; i++) {
      await page.mouse.wheel(0, 4000);
      await randomDelay(800, 1400);
      const clicked = await clickLoadMoreIfPresent(page);
      if (!clicked) {
        process.stderr.write(
          `[rt/browse] no Load More button at iter ${i}; stopping pagination\n`,
        );
        break;
      }
      await randomDelay(1500, 2800);
    }

    const items = await extractTiles(page);
    process.stderr.write(`[rt/browse] extracted ${items.length} tiles\n`);
    return { sort, collectedAt: new Date().toISOString(), items };
  } finally {
    await context.close();
    await browser.close();
  }
}

async function main() {
  const [, , method, headless, argsJson] = process.argv;
  if (!method) {
    process.stderr.write(
      "Usage: rottentomatoes-scraper.ts <method> <headless> [argsJson]\n",
    );
    process.exit(2);
  }
  const args = argsJson ? JSON.parse(argsJson) : {};
  let result;
  switch (method) {
    case "browse":
      result = await browse(headless, args);
      break;
    default:
      process.stderr.write(`Unknown method: ${method}\n`);
      process.exit(2);
  }
  process.stdout.write(JSON.stringify(result));
}

main().catch((err) => {
  process.stderr.write(
    `[rt/scraper] ${err.stack || err.message || String(err)}\n`,
  );
  process.exit(1);
});
