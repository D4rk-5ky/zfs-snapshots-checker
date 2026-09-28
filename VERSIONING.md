# Versioning

This project uses three-part versions and increments each created release by exactly `0.0.1`. After `x.y.99`, the next release rolls over to `x.(y+1).0`; for example, `0.0.99` becomes `0.1.0` rather than `0.0.100`.

## 2.4.3

Current release.

Code changes:

- Fixed the interaction between `--only-non-sanoid` and `--write-destroy-script`. In 2.4.2, `--only-non-sanoid` filtered only displayed datasets while destroy-script generation still silently used the default `candidates` mode, which could write off-schedule weekly/monthly Sanoid snapshots.
- When `--write-destroy-script` is used with `--only-non-sanoid` and no explicit `--write-destroy-script-mode`, the effective mode now becomes `non-sanoid`.
- Preserved explicit mode control: `--write-destroy-script-mode candidates`, `non-sanoid`, or `both` always overrides the contextual default.
- Preserved the existing default `candidates` mode when `--only-non-sanoid` is not present.
- Ignore-list filtering remains upstream of classification, so ignored names such as `SnapBeforeWatchTower...` are still excluded from non-Sanoid output and destroy-script generation.
- Updated the shared application version from `2.4.2` to `2.4.3`.

Documentation/project-maintenance changes:

- Updated `--help` so both `--only-non-sanoid` and `--write-destroy-script-mode` describe the contextual default clearly.
- Updated `README.md` with the corrected interaction and an example matching the reported command.
- Updated `commented_code_map.md` so the CLI behavior and `main()` orchestration description match the implementation.

## 2.4.2

Previous release.

Code changes:

- Added `--recursively`, valid with `--only-dataset`, to analyze the selected dataset and descendant datasets that are already explicitly listed in `dataset_file`. This preserves the dataset file as an allow-list and does not auto-discover unrelated ZFS datasets.
- Added `--ignore-list-file FILE`. Each nonblank, non-comment line is a case-sensitive literal substring matched against the snapshot name after `@`.
- Ignored snapshots are filtered before parsing/counting, so they are excluded from total counts, Sanoid/non-Sanoid classification, newest/stale calculations, off-schedule checks, cleanup candidates, JSON results, and destroy-script generation.
- Replaced the dataset-specific line reader with reusable `read_list_file()` so both dataset and ignore-list files share the same blank/comment handling without duplicate parsing code.
- Added `select_datasets()` to keep exact and recursive dataset selection logic centralized.
- Added `snapshot_matches_ignore()` to centralize snapshot-name ignore matching.
- Updated the shared application version from `2.4.1` to `2.4.2`.

Documentation/project-maintenance changes:

- Updated `README.md` with recursive dataset selection, ignore-list semantics, examples, safety behavior, and current exit conditions.
- Added `snapshot_ignore_file.example` showing how to ignore names containing `SnapBeforeWatchTower`.
- Updated `commented_code_map.md` for all current functions, flags, and command behavior.

## 2.4.1

Previous release.

Code changes:

- Added the single `VERSION = "2.4.1"` application version constant.
- Added `--version` to the existing `argparse` CLI so the program exposes its current version without requiring a dataset file or Sanoid configuration.
- Updated the CLI description to use the shared version constant instead of the hard-coded legacy `v2.4` text.
- Expanded built-in help for destroy-script options so it explicitly states that the checker writes commands for manual review and never executes `zfs destroy` itself.
- Updated generated destroy-script headers to identify `zfs-snapshots-checker.py` and the current shared version instead of the obsolete `zfs_sanoid_check_v2_4.py` filename.
- Added a CLI safety epilog reinforcing that generated destroy scripts are not executed by the checker.

Documentation/project-maintenance changes:

- Rebuilt `README.md` around the current `zfs-snapshots-checker.py` filename and actual current behavior, commands, filters, Sanoid policy resolution, output behavior, safety workflow, and exit behavior.
- Added the required Disclaimer / Liability and AI-assisted software disclaimer text.
- Added `commented_code_map.md` documenting every current dataclass, function, CLI argument/flag, and external command used by the program.
- Converted the empty `datasets_file` placeholder into a commented example/template without adding any active dataset entries.
- Added this `VERSIONING.md` file.

## 2.4

Legacy baseline version inherited from the uploaded project. The source identified itself as `v2.4`, but the uploaded archive contained no version-history file, so earlier change history is intentionally not invented here.
