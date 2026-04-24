# Readwise Reader Sync for Logseq

Unofficial Logseq plugin for syncing Readwise Reader highlights into managed Logseq pages.

This project is published as an independent plugin release line. If it is published to the Logseq marketplace, it should be labeled `Unofficial` to avoid confusion with upstream community plugins and Readwise's official implementation.

## Current Status

- Formal sync uses Reader v3 incremental highlight sync by default.
- The plugin groups highlights by `parent_id` and rewrites managed pages under `ReadwiseHighlights/<title>`.
- Managed page identity is `rw-reader-id`, not page title alone.
- The plugin records the current Reader sync cursor and latest formal sync summary in `Readwise Sync State`.
- Legacy checkpoint state, if needed, lives in `Readwise Legacy Sync State`.
- Auto Sync is available with saved-cursor gating, suspicious-run confirmation, and active-page write deferral. The guardrail rationale is documented in [docs/auto-sync-guardrails-spec.md](./docs/auto-sync-guardrails-spec.md).

## Features

- Reader v3 incremental sync with explicit progress, ETA, and structured logging
- Optional Auto Sync for day-to-day incremental runs, with confirmation before suspiciously large page rewrites
- Manual `Full Refresh` for whole-library rebuilds
- `Refresh Local Snapshot Only` for rebuilding the local full-library snapshot without rewriting managed pages
- `Cached Full Rebuild` for rewriting every managed page from the local snapshot without another full remote highlight scan
- Cache-first legacy managed-page migration preview, with an in-panel apply step
- Dedicated legacy managed-page apply report, including bind/rename/rebuild follow-up details
- Current-page tools for `Rebuild Current Page From Cache` and `Refresh Current Page Metadata`
- Preview-first current-page legacy ID migration for pages and whiteboards
- Dedicated current-page preview and apply summaries, instead of burying successful previews in the issue list
- Managed page identity tracking through `rw-reader-id`
- Automatic page retargeting when a managed page title changes
- Maintenance tools grouped into `Audit & Repair`, `Migration`, `Snapshots`, `Test & Preview`, and `Debug`
- Formal sync summary page with counts and phase timings
- Log level control through plugin settings

## Known Tradeoff

`Incremental Sync` uses the saved Reader cursor and only scans changed highlights and notes. When it rewrites a changed page, it rebuilds that page by merging the changed highlights onto the local cached highlight snapshot for the same `parent_id`. `Full Refresh` still scans the full Reader highlight and note library and can take minutes on large libraries. `Refresh Local Snapshot Only` runs the same full remote scan too, but stops after refreshing the local snapshot and does not rewrite managed pages. `Cached Full Rebuild` is the complementary maintenance action: it skips the remote highlight scan, reuses the local cached highlight snapshot, prefers cached parent metadata, only refetches missing parent metadata from Reader, and rewrites every managed page. Legacy managed-page migration preview is intentionally cache-first: it uses cached Reader metadata and cached highlights to prove safe rebindings, and leaves unresolved pages as warnings instead of remote-scanning the Reader library just for preview. Auto Sync only runs `Incremental Sync`; it never promotes itself to `Full Refresh` or snapshot-only maintenance work, and it now stays out of the way while an interactive preview/apply workflow is still pending in the foreground. Current-page tools rely on the local highlight snapshot; if a debug highlight-page cap truncates a full-library scan, the plugin keeps the previous cached snapshot instead of replacing it with a partial one.

## Sync Scope Difference From The Official Plugin

- Readwise's official Logseq plugin syncs all tweets by default, even when a tweet has no highlight.
- This project currently syncs parent documents only when they have at least one Reader highlight.
- That rule applies across document types, including `article`, `book`, `tweet`, and `video`.

## Install From Release

1. Download the latest `logseq-readwise-reader-sync.zip` from [Releases](https://github.com/FelixHuoEZ/logseq-readwise-reader-sync/releases).
2. Extract the archive. It should produce a `logseq-readwise-reader-sync/` folder.
3. In Logseq, open `Plugins`.
4. Choose `Load unpacked plugin`.
5. Select the extracted `logseq-readwise-reader-sync/` folder.

## Configure

1. Open plugin settings.
2. Paste your Readwise access token from [readwise.io/access_token](https://readwise.io/access_token).
3. Leave the Automation section at defaults for normal use:
   - `Enable Auto Sync = on`
   - `Auto Sync Interval (minutes) = 15`
4. Auto Sync only arms itself after one successful manual `Incremental Sync` or `Full Refresh` has established a saved cursor.
5. Leave the Debug section at defaults for normal use:
   - `Log Level = warn`
   - `Reader Full Scan Target Documents = 20`
   - `Reader Full Scan Debug Highlight Page Limit = 0`

Do not run this project and another Readwise Logseq plugin against the same graph at the same time. They share managed page namespaces and properties even though the plugin package id is distinct.

## Use

1. Open the plugin panel.
2. Click `Incremental Sync` for the normal day-to-day path.
3. Use `Full Refresh` when you need a fresh whole-library rebuild.
4. Leave Auto Sync enabled if you want the plugin to periodically check whether a safe automatic `Incremental Sync` should run.
5. If Auto Sync detects a suspiciously large rewrite set, review the confirmation dialog before allowing page writes to continue.
6. Open `Maintenance Tools > Snapshots` and use `Refresh Local Snapshot Only` when you want a fresh local snapshot without rewriting pages.
7. Use `Maintenance Tools > Snapshots > Cached Full Rebuild` when you want to rewrite every managed page from the local snapshot without another full remote highlight scan.
8. Use `Rebuild Current Page From Cache` when a single managed page needs a local rebuild.
9. Use `Refresh Current Page Metadata` when a single managed page needs fresh parent metadata from Reader.
10. Open `Maintenance Tools > Migration` when you need low-frequency legacy id workflows.
11. Use `Preview Legacy Managed Page Migration` to inspect safe legacy page rebindings from cached Reader metadata before applying them. Tweet-only pages without `View Highlight` are skipped.
12. Use `Preview Current Page Legacy ID Migration` to inspect legacy Readwise id rewrites on the current page or whiteboard before applying them.
13. If the preview looks right, apply it from the same `Migration` section before leaving the workflow.
14. Wait for the plugin to:
   - scan Reader highlights and notes, or load the cached highlight snapshot
   - group them by parent document
   - fetch the target parent documents
   - rewrite managed pages

After each formal sync:

- `Readwise Sync State` shows the latest formal sync summary.
- `Readwise Sync State` also stores the current Reader incremental cursor.
- `ReadwiseHighlights/<title>` pages are updated in place.
- Page identity continues to follow `rw-reader-id`.

## Managed Page Model

- Formal pages live in `ReadwiseHighlights/<title>`.
- The plugin first resolves a page by `rw-reader-id`.
- If no page matches by `rw-reader-id`, it falls back to the managed title.
- If the managed title changed, the plugin renames the matched managed page to the current tracked title.

## Debug And Recovery

The panel hides maintenance tools during normal use. They appear when formal sync detects conflicting managed pages, or when you explicitly expose them during debugging.

Maintenance tools are grouped by purpose:

- `Audit & Repair`
  - cache inspection, managed-id audit, damaged-page repair
- `Migration`
  - current-page legacy id preview/apply
  - graph-wide legacy block ref preview/apply
- `Snapshots`
  - refresh the local highlight snapshot without rewriting pages
  - rebuild every managed page from the local snapshot without another full remote highlight scan
  - force managed pages through the legacy touch-and-restore reparse helper
  - capture and diff raw current-page snapshots
- `Test & Preview`
  - session test pages, preview pages, and restore/clear helpers
- `Debug`
  - short-lived debug sync pages and cleanup
  - experimental current-page internal reparse probe that reads the current page from disk and fails closed when Logseq does not expose a callable private watcher or `file/alter` bridge

Current internal-reparse POC status:

- In the inspected Logseq desktop bundle, the `file/alter` event path calls Logseq's internal `alter_file` with `from-disk?` and should not write the page file back to disk.
- In the current iframe plugin runtime, the page file can be read through Logseq's host `load_file` helper, but the private watcher handler and `pub_event!` bridge are not exposed to the plugin iframe. Treat this button as a diagnostic probe, not a reliable workaround, unless a future Logseq build exposes one of those bridges.

Auto Sync itself is configured in plugin settings, not inside `Maintenance Tools`. Its protection flow still matters during debugging:

- automatic runs only arm after a saved cursor exists
- suspiciously large automatic rewrite sets pause before page writes and ask for confirmation
- automatic writes to pages that are open or recently active are deferred instead of being forced immediately

Debug settings affect different phases:

- `Reader Full Scan Target Documents`
  - limits how many managed pages `Full Refresh` rewrites
  - mainly reduces parent-document fetch and page-write time
- `Reader Full Scan Debug Highlight Page Limit`
  - limits how many Reader highlight and note pages `Incremental Sync` and `Full Refresh` scan
  - mainly reduces remote-scan time
  - roughly `100` Reader items per page
  - if it truncates `Full Refresh`, the local cached highlight snapshot is not refreshed

Use the highlight page limit only for short debug runs. Set it back to `0` for real formal sync.

`Repair Managed Pages` uses API-authoritative recovery for the hard cases:

- If a damaged page has no `rw-reader-id`, but still has `View Highlight` links, repair re-looks up the parent through the Reader API instead of trusting cache alone.
- If a damaged page already has `rw-reader-id`, but its cached highlights are missing, repair now tries to reload the page's highlight documents directly from the embedded `View Highlight` links before giving up.
- If a damaged page's `rw-reader-id` can be recovered, but Reader no longer returns rebuildable highlights for that document, repair now falls back to metadata-only orphan recovery: it refreshes page-level metadata, preserves the existing local highlight body, and normalizes legacy highlight block ids instead of failing as `no rebuildable highlights`.
- If a damaged page reloads sparse tweet/media highlights, repair now forces a Reader detail-enrichment pass before writing so image-first highlights do not collapse into blank headings.
- Repair no longer falls back to a full Reader `article` replacement scan just to guess a missing parent document during this maintenance pass.
- If Reader still does not provide a unique, high-confidence parent, the page stays as an issue instead of being rebound by guesswork.
- Damaged-page rewrites now force a follow-up file reparse after the exact rewrite so duplicate page preludes do not reappear after repair.
- Legacy managed page migration now follows a preview-first flow with tweet-only filtering: pages that still contain `View Highlight` stay in scope even if they are tweets, while tweet-only pages without `View Highlight` are skipped from this migration flow.
- The apply step now emits a dedicated apply report that records which pages were bound, renamed, rebuilt, or still need manual follow-up.
- Applying a legacy managed page migration now prefers local cache, but can also reload a page's highlights directly from Reader when the old page still contains `View Highlight` links. If the document id can be recovered but current remote highlights are gone, apply now uses the same metadata-only orphan fallback instead of hard-failing the page.
- Legacy block ref migration now follows a preview-first flow: the plugin lists every planned `((block ref))` rewrite before you confirm the apply step.
- Current-page legacy id migration also follows a preview-first flow and rewrites only the current page or whiteboard. It updates proven Readwise legacy ids in `((block refs))`, whiteboard embeds, and `:refdock-item-id:` values, then shows a dedicated apply summary after the rewrite completes.

See [docs/repair-managed-pages-spec.md](./docs/repair-managed-pages-spec.md) for the detailed recovery rules, analyzed failure classes, and the guardrails for these repair flows.

For release-time validation, see [docs/manual-regression-checklist.md](./docs/manual-regression-checklist.md).

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

This project builds on work from and explicitly references ideas from:

- [hkgnp/logseq-readwise-plugin](https://github.com/hkgnp/logseq-readwise-plugin), for the original community plugin line
- [benjypng/logseq-readwise-plugin](https://github.com/benjypng/logseq-readwise-plugin), for the local-first base and sync architecture direction
- [readwiseio/logseq-readwise-official-plugin](https://github.com/readwiseio/logseq-readwise-official-plugin), for output compatibility targets and export integration ideas

## Thanks

- Thanks to the original community maintainers and contributors for the earlier plugin lines.
- Thanks to Readwise for the official plugin implementation and export behavior that informed compatibility work in this project.

This project does not claim affiliation with Readwise or the original community maintainers. It is a separately maintained release line.

## License

[MIT](./LICENSE.md)
