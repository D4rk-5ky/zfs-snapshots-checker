# Commented Code Map

This file maps the current `zfs-snapshots-checker.py` implementation. It explains what each class, function, CLI command/flag, and external command does and why it exists.

## Data structures

| Item | Purpose | Why it exists |
| --- | --- | --- |
| `SnapshotInfo` | Stores one parsed snapshot: full name, dataset, snapshot name, whether it matches Sanoid naming, optional Sanoid type, and parsed timestamp. | Gives later checks one consistent representation instead of repeatedly reparsing snapshot names. |
| `DatasetPolicy` | Stores the resolved Sanoid policy for one dataset, including source section, recursive inheritance, template, autosnap/autoprune/recursive booleans, desired counts, schedule fields, and merged raw values. | Keeps policy resolution separate from snapshot analysis and preserves enough detail for reporting. |
| `DatasetResult` | Stores all findings for one dataset: error, policy, counts, non-Sanoid snapshots, off-schedule snapshots, newest snapshots, stale/excess reasons, and cleanup candidates. | Lets analysis, filtering, text output, JSON output, and destroy-script generation reuse the same result object. |

## Constants

| Item | Purpose |
| --- | --- |
| `SANOID_REGEX` | Recognizes Sanoid snapshot names in the form `autosnap_YYYY-MM-DD_HH:MM:SS_TYPE`. |
| `SNAP_TYPES` | Defines the supported Sanoid classes: frequently, hourly, daily, weekly, monthly, yearly. |
| `VERSION` | Single source for the application version used by `--version`, CLI description, and generated-script metadata. |

## Functions

| Function | What it does | Why it exists |
| --- | --- | --- |
| `run_command(cmd)` | Runs an external command with captured text stdout/stderr and returns `(returncode, stdout, stderr)`; converts Python execution exceptions into a nonzero result. | Centralizes subprocess handling so ZFS callers receive a uniform result without duplicating subprocess code. |
| `parse_bool(value)` | Converts common Sanoid-style true/false strings (`yes/no`, `true/false`, `1/0`, `on/off`) to `True`, `False`, or `None`. | Sanoid configuration values are strings; later policy logic needs normalized booleans. |
| `parse_int(value)` | Tries to convert a config value to `int`, returning `None` when absent/invalid. | Allows schedule/count parsing without raising on malformed or missing optional values. |
| `read_list_file(path)` | Reads nonblank, non-comment lines from a plain-text list file. | Reuses one parser for both dataset allow-lists and snapshot ignore lists instead of duplicating line-handling code. |
| `select_datasets(datasets, only_dataset, recursively)` | Selects the requested exact dataset, or that dataset plus descendants whose names start with `DATASET/`; only entries already present in the input list can be selected. | Implements `--only-dataset --recursively` while preserving `dataset_file` as the explicit allow-list. |
| `load_ini(path)` | Loads an INI file with interpolation disabled, duplicate sections/options tolerated by `strict=False`, and original option case preserved. | Reads Sanoid files without ConfigParser interpolation changing values and matches the project's existing tolerant behavior. |
| `extract_template_name(section_name)` | Returns the suffix after `template_`, or `None` for non-template sections. | Separates Sanoid template sections from dataset sections during map construction. |
| `build_maps(sanoid_cfg, defaults_cfg)` | Builds three maps: named templates, dataset sections, and merged `template_default`; defaults are loaded first and `sanoid.conf` overrides them. | Produces normalized lookup structures used by policy resolution. |
| `parent_datasets(dataset)` | Returns the dataset and its parents from most specific to least specific. | Supports searching upward for the nearest recursive Sanoid section. |
| `resolve_policy(dataset, dataset_sections, templates, default_template)` | Finds an exact or recursive parent section, merges default/template/section settings, normalizes booleans/counts/schedule values, and returns `DatasetPolicy`. | Encapsulates Sanoid inheritance/precedence logic so snapshot analysis operates on one resolved policy. |
| `list_snapshots_for_dataset(dataset)` | Runs recursive `zfs list` for snapshots, then keeps only snapshots whose dataset component exactly equals the requested dataset. | `zfs list -r` can include child datasets; exact filtering prevents child snapshots from contaminating the parent result. |
| `snapshot_matches_ignore(snapshot_full_name, ignore_patterns)` | Checks whether the snapshot-name portion after `@` contains any configured case-sensitive ignore substring. | Applies one consistent ignore rule before snapshots enter counts, findings, JSON, or destroy-script candidate logic. |
| `parse_snapshot(snapshot_full_name)` | Splits `dataset@snapshot`, matches Sanoid naming, parses type/timestamp, and returns `SnapshotInfo`. | Converts raw ZFS names into structured data used by all checks. |
| `newest_snapshot_by_type(snapshots)` | Finds the newest timestamped Sanoid snapshot for each supported type. | Staleness checks need the latest snapshot in each configured class. |
| `format_timedelta(delta)` | Formats an age as days/hours/minutes and clamps negative ages to zero. | Produces readable stale-snapshot messages while avoiding confusing negative output from future-dated snapshots. |
| `stale_thresholds_from_policy(policy)` | Builds fixed staleness thresholds only for configured hourly/daily/weekly/monthly/yearly classes. | Prevents stale checks for classes with a desired count of zero and centralizes threshold values. |
| `find_stale_autosnap_reasons(policy, newest, now)` | For `autosnap=True`, reports configured classes that are missing or whose newest snapshot exceeds the fixed threshold. | Detects likely Sanoid snapshot-generation gaps. |
| `find_exceeds(policy, counts)` | Reports hourly/daily/weekly/monthly/yearly counts greater than configured desired counts. | Detects retention/pruning results that exceed policy. |
| `is_offschedule(snapshot, policy)` | Checks weekly weekday/hour/minute or monthly day/hour/minute against the resolved schedule; other types currently return false. | Identifies weekly/monthly Sanoid snapshots likely created outside the expected schedule. |
| `find_likely_manual_cleanup_candidates(policy, snapshots)` | Returns sorted unique off-schedule weekly/monthly Sanoid snapshot names. | Reuses schedule logic to define the project's current candidate set for optional destroy-script generation. |
| `snapshot_exists(full_snapshot_name)` | Runs an exact `zfs list` lookup and distinguishes a normal not-found condition from an unexpected lookup error. | Preflight generation must avoid activating destroy commands for snapshots that disappeared or cannot be verified. |
| `snapshot_has_holds(full_snapshot_name)` | Runs `zfs holds`, parses hold tags, and returns held/not-held/unknown. | ZFS holds can prevent or intentionally protect deletion; preflight generation comments held or unverifiable snapshots out. |
| `analyze_dataset(dataset, policy, now, ignore_patterns=None)` | Lists snapshots, removes names matching configured ignore substrings, then parses the remaining snapshots and fills counts, classifications, newest/stale/excess findings, and cleanup candidates. | Ensures ignored snapshots are excluded once, before every downstream analysis/output path. |
| `serialize_result(result)` | Converts dataclasses/datetimes into JSON-serializable dictionaries. | Keeps JSON formatting separate from analysis logic. |
| `should_print(result, ...)` | Applies the selected report filter and error visibility rules; without an `--only-*` filter, prints findings/errors, or everything with `--show-ok`. | Avoids duplicating filter logic in the CLI loop. Multiple `--only-*` flags are not combined; the fixed function order determines precedence. |
| `print_result(result)` | Prints the human-readable policy/count/newest/finding report for one dataset. | Isolates presentation from analysis. |
| `write_destroy_script(output_path, results, mode, append, dry_run_check)` | Selects candidate/non-Sanoid snapshots, optionally preflights existence/holds, writes active or commented `zfs destroy` lines, and returns the number of active commands. | Generates a reviewable cleanup artifact without executing destructive commands. |
| `build_parser()` | Builds the complete `argparse` CLI, including version/help text, positional input, report filters, destroy-script controls, and safety epilog. | Keeps CLI definition centralized and makes `--help` authoritative. |
| `main()` | Parses arguments, validates inputs, loads Sanoid files, resolves/analyzes each requested dataset, optionally writes a destroy script, then prints JSON or filtered text output and returns the process status. | Coordinates the program's existing top-level workflow while delegating specialized work to reusable functions. |

## CLI arguments and commands

Run the program as:

```bash
python3 zfs-snapshots-checker.py DATASET_FILE --configdir DIR [OPTIONS]
```

| Argument / flag | Effect |
| --- | --- |
| `dataset_file` | Required file containing one dataset per non-comment line. |
| `--configdir` | Required option whose `DIR` value is the Sanoid config directory containing `sanoid.conf` and optional `sanoid.defaults.conf`. |
| `--json` | Emits analyzed results as formatted JSON instead of normal dataset text. |
| `--show-ok` | Includes datasets with no findings in normal text output. |
| `--only-stale` | Filters normal text output to stale/missing autosnap findings and errors. |
| `--only-exceeds` | Filters to retention-count excess findings and errors. |
| `--only-non-sanoid` | Filters to datasets with non-Sanoid snapshots and errors. |
| `--only-offsched` | Filters to off-schedule weekly/monthly Sanoid snapshots and errors. |
| `--only-cleanup-candidates` | Filters to likely manual cleanup candidates and errors. |
| `--only-dataset` | Takes a `DATASET` value and limits analysis to that exact dataset already present in the dataset file; with `--recursively`, descendants from that same file are included. |
| `--recursively` | Requires `--only-dataset`; includes the selected dataset and descendant dataset names already listed in `dataset_file`. |
| `--ignore-list-file` | Takes a `FILE`; each non-comment line is a case-sensitive literal substring matched against the snapshot name after `@`. Matching snapshots are excluded before all counting/report/destroy logic. |
| `--write-destroy-script` | Takes a `FILE` path and writes selected destroy commands there for manual review; it never executes them. |
| `--write-destroy-script-mode` | Takes `candidates`, `non-sanoid`, or `both`; `candidates` writes current manual cleanup candidates and is the default mode. |
| `--append-destroy-script` | Appends when a destroy script is being written instead of replacing it. |
| `--dry-run-destroy-check` | Preflights existence and holds before leaving generated destroy lines active. |
| `--version` | Prints the shared `VERSION` and exits. |
| `-h`, `--help` | Prints all parser-generated usage/help text and exits. |

## External commands executed by Python

| Command form | Function | Purpose / safety behavior |
| --- | --- | --- |
| `zfs list -H -t snapshot -o name -r DATASET` | `list_snapshots_for_dataset()` | Reads snapshot names. No destructive action. Child results are filtered out after the command. |
| `zfs list -H -t snapshot -o name DATASET@SNAP` | `snapshot_exists()` | Exact existence check used by destroy-script preflight. No destructive action. |
| `zfs holds DATASET@SNAP` | `snapshot_has_holds()` | Reads hold tags used by destroy-script preflight. No destructive action. |

## Commands written but never executed by Python

| Generated command | Where | Purpose / safety behavior |
| --- | --- | --- |
| `zfs destroy 'DATASET@SNAP'` | `write_destroy_script()` | Written to the requested shell script for manual review. With preflight enabled, missing, held, or unverifiable snapshots are emitted only as commented commands. The Python checker never invokes `zfs destroy`. |

## Top-level execution

`if __name__ == "__main__": sys.exit(main())` makes the file directly executable as a CLI and propagates `main()`'s status code to the shell.
