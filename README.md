# Readwise Reader Sync for Logseq

Unofficial Logseq plugin for syncing Readwise Reader highlights into managed Logseq pages.

This fork is published as an independent plugin release line. If it is published to the Logseq marketplace, it should be labeled `Unofficial` to avoid confusion with upstream community plugins and Readwise's official implementation.

## Current Status

- Formal sync uses Reader v3 highlight scans.
- The plugin groups highlights by `parent_id` and rewrites managed pages under `ReadwiseHighlights/<title>`.
- Managed page identity is `rw-reader-id`, not page title alone.
- The plugin records the latest formal sync summary in `Readwise Sync State`.
- Legacy checkpoint state, if needed, lives in `Readwise Legacy Sync State`.

## Features

- Reader v3 formal sync with explicit progress, ETA, and structured logging
- Managed page identity tracking through `rw-reader-id`
- Automatic page retargeting when a managed page title changes
- Hidden maintenance tools for debug, preview, and recovery flows
- Formal sync summary page with counts and phase timings
- Log level control through plugin settings

## Known Tradeoff

Formal sync currently scans the Reader highlight library before grouping by parent document. Large libraries can take minutes to fetch. For debug runs, reduce the target document count and add a temporary highlight page cap in plugin settings.

## Install From Release

1. Download the latest `logseq-readwise-reader-sync.zip` from [Releases](https://github.com/FelixHuoEZ/logseq-readwise-reader-sync/releases).
2. Extract the archive. It should produce a `logseq-readwise-reader-sync/` folder.
3. In Logseq, open `Plugins`.
4. Choose `Load unpacked plugin`.
5. Select the extracted `logseq-readwise-reader-sync/` folder.

## Configure

1. Open plugin settings.
2. Paste your Readwise access token from [readwise.io/access_token](https://readwise.io/access_token).
3. Leave the Debug section at defaults for normal use:
   - `Log Level = warn`
   - `Reader Full Scan Target Documents = 20`
   - `Reader Full Scan Debug Highlight Page Limit = 0`

Do not run this fork and another Readwise Logseq plugin against the same graph at the same time. They share managed page namespaces and properties even though the plugin package id is distinct.

## Use

1. Open the plugin panel.
2. Click `Start Sync`.
3. Wait for the plugin to:
   - scan Reader highlights
   - group them by parent document
   - fetch the target parent documents
   - rewrite managed pages

After each formal sync:

- `Readwise Sync State` shows the latest formal sync summary.
- `ReadwiseHighlights/<title>` pages are updated in place.
- Page identity continues to follow `rw-reader-id`.

## Managed Page Model

- Formal pages live in `ReadwiseHighlights/<title>`.
- The plugin first resolves a page by `rw-reader-id`.
- If no page matches by `rw-reader-id`, it falls back to the managed title.
- If the managed title changed, the plugin renames the matched managed page to the current tracked title.

## Debug And Recovery

The panel hides maintenance tools during normal use. They appear when formal sync detects conflicting managed pages, or when you explicitly expose them during debugging.

Debug settings affect different phases:

- `Reader Full Scan Target Documents`
  - limits how many managed pages formal sync rewrites
  - mainly reduces parent-document fetch and page-write time
- `Reader Full Scan Debug Highlight Page Limit`
  - limits how many Reader highlight pages formal sync scans
  - mainly reduces highlight-scan time
  - roughly `100` highlights per page

Use the highlight page limit only for short debug runs. Set it back to `0` for real formal sync.

## Development

```bash
npm install
npm run build
```

To build a release-ready archive locally:

```bash
npm run package:plugin
```

This command creates `logseq-readwise-reader-sync.zip` in the repository root.

## Author

- Maintained by [FelixHuoEZ](https://github.com/FelixHuoEZ)
- Built and iterated with Codex

## References

This fork builds on work from and explicitly references ideas from:

- [hkgnp/logseq-readwise-plugin](https://github.com/hkgnp/logseq-readwise-plugin), for the original community plugin line
- [benjypng/logseq-readwise-plugin](https://github.com/benjypng/logseq-readwise-plugin), for the local-first fork base and sync architecture direction
- [readwiseio/logseq-readwise-official-plugin](https://github.com/readwiseio/logseq-readwise-official-plugin), for output compatibility targets and export integration ideas

## Thanks

- Thanks to the original community maintainers and contributors for the earlier plugin lines.
- Thanks to Readwise for the official plugin implementation and export behavior that informed compatibility work in this fork.

This fork does not claim affiliation with Readwise or the original community maintainers. It is a separately maintained release line.

## License

[MIT](./LICENSE.md)
