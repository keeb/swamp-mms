# Property-based testing for swamp extensions

Investigation of `zod-fast-check` (2026-07-18): generate fast-check
arbitraries from the zod schemas each model already declares, so every
generated input is by construction "valid" per the model's own validator,
then assert the surrounding code actually handles the full valid input
space.

## Library verdict

| Library | Verdict |
| --- | --- |
| `zod-fast-check` | **Unusable.** Last release 0.10.1 (Sep 2023), peer-dep `zod ^3.18.0`. Swamp extensions must use `npm:zod@4`, whose schema internals (`_zod.def`) are incompatible with the zod-3 internals it walks. |
| `@traversable/zod-test` | **Works.** Same concept rebuilt for zod 4 (`peerDependencies: zod 4, fast-check 3-4`). `zxTest.fuzz(schema)` → fast-check arbitrary of schema-valid data. Verified under swamp's bundled deno. Caveat: pre-1.0 (0.0.28), and deno doesn't auto-install its peer deps — import `@traversable/zod-types` and `@traversable/registry` explicitly first. |

## What's here

- `schemas_test.ts` — **generic harness, identical across extensions.**
  Discovers every model in `extensions/models/`, derives an arbitrary from
  each declared schema (`globalArguments`, method `arguments`, resource
  schemas), and asserts generated data round-trips through the schema's own
  `safeParse`. Zero per-model code.
- `dedup_test.ts` — **deep dive** for `@keeb/mms/dedup`: properties over the
  exported pure helpers (`normalize`, `slugDir`, `fileExistsInIndex`,
  `fileExistsInDest`, `pickPreferred`), driven by arbitraries derived from
  `EpisodeSchema`.

Run with:

```sh
~/.swamp/deno/deno test -A tests/property/
```

## Did it find anything real?

Yes, immediately. `EpisodeSchema` declares `episode: z.string()` — any
string is valid — but `fileExistsInIndex`/`fileExistsInDest` interpolated
the episode string raw into `new RegExp(...)`. First run of the totality
property crashed with `SyntaxError: Invalid regular expression` and
fast-check shrank the counterexample to `episode: "("`. Episode strings come
from RSS titles and LLM parses, so this was reachable in production. Fixed
with `escapeRegex()` in `mms_dedup.ts`.

## Known limitations

- `@traversable/zod-test` does not honor string *format* refinements
  (`.datetime()`, `.uuid()`, `.email()`, …), custom
  `.refine()`/`.superRefine()` rules, or array/string size bounds
  (`.min()`/`.max()`) — it generates values the schema then rejects. The
  harness skips samples whose only parse errors are
  `invalid_format`/`custom`/`too_small`/`too_big` and prints a note.
  Schemas where every sample skips (e.g. resource schemas with a required
  datetime field, or resend's `send` refinement) get **no effective
  coverage** from the generic harness.
- The generic harness only proves schemas are self-consistent and
  generatable. The real value (the dedup deep dive) requires the model file
  to export its pure helpers so tests can drive them — worth doing whenever
  a model grows non-trivial pure logic.
- Pin all three packages exactly (`fast-check@4.6.0`,
  `@traversable/zod-test@0.0.28`, `zod-types@0.0.30`, `registry@0.0.49`);
  the traversable packages are pre-1.0 and move together.
