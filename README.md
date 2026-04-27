# @keeb/mms

A swamp extension that stitches together a small media management pipeline:
search torrent and usenet sources, filter out episodes that already exist on
disk, hand off new downloads to Transmission or SABnzbd, and organize
completed files with an Ollama-powered classifier. The extension ships four
model types and three reports.

Models:

- `@keeb/mms/source` — search SubsPlease, Nyaa, EZTV, and Newznab feeds.
- `@keeb/mms/dedup` — filter discovered episodes against local directories.
- `@keeb/mms/downloader` — add, list, remove, and clean Transmission/SABnzbd downloads.
- `@keeb/mms/organizer` — pop MongoDB-queued jobs and move files with LLM-picked paths.

Reports: `@keeb/mms/discovery-summary`, `@keeb/mms/dedup-summary`,
`@keeb/mms/download-status`.

## Installation

```sh
swamp extension install @keeb/mms
```

## Usage

A minimal definition that searches for an anime on Nyaa, filters out episodes
already on disk, and adds the new ones to Transmission:

```yaml
models:
  - name: src
    type: "@keeb/mms/source"
    globalArguments:
      shows:
        - name: "One Piece"
          provider: nyaa
          resolution: "1080"
          nyaaUser: "Erai-raws"

  - name: dedup
    type: "@keeb/mms/dedup"
    globalArguments:
      checkDirs: ["/home/keeb/media/anime"]
      ollamaUrl: "http://localhost:11434"
      ollamaModel: "qwen3:14b"

  - name: dl
    type: "@keeb/mms/downloader"
    globalArguments:
      transmissionUrl: "http://transmission.lan:9091"
      transmissionUser: "${{ vault.transmission.user }}"
      transmissionPassword: "${{ vault.transmission.password }}"
```

```sh
swamp model method run src search_configured
swamp model method run dedup filter --args '{"sourceModel":"src"}'
swamp model method run dl status
```

## How it works

Each model writes typed resources that the next stage reads via
`context.readModelData`. `mms_source` normalizes SubsPlease, Nyaa, EZTV, and
Newznab feeds into an `episode` resource with a `magnet` (or NZB URL) and
optional `infoHash`, `seeders`, and `protocol`. `mms_dedup` first tries a set
of regex patterns (`S01E06`, `1x06`, anime-style `Title - 06`) against raw
titles, and falls back to Ollama only when the regex can't parse; it then
checks `{checkDir}/{show-slug}/s{season}/...` and flat staging paths before
emitting surviving episodes. `mms_downloader` speaks Transmission RPC (with
session-id handling) for torrents and SABnzbd's HTTP API for usenet.
`mms_organizer` pops pending jobs from a MongoDB `jobs` collection, runs two
LLM stages (filename → metadata JSON, metadata → destination path), and moves
files, falling back to a cross-device copy when rename fails.
