# ZFS Snapshots Checker

`zfs-snapshots-checker.py` audits ZFS snapshots against Sanoid configuration. It can report stale/missing Sanoid snapshots, counts above configured retention, non-Sanoid snapshots, and weekly/monthly Sanoid snapshots that do not match the configured schedule. It can also generate a reviewable shell script containing selected `zfs destroy` commands.

The checker itself **does not execute `zfs destroy`**. A generated destroy script is destructive if you later run it.

## ⚠️ Disclaimer / Liability

**Use this script at your own risk.**

The author takes **no responsibility or liability** for any data loss, service disruption, misconfiguration, service outage, missed backups, credential exposure, or other damage that may occur from using this script.

Before running it in production, you **must**:

- Read the entire source code
- Understand exactly what it does (and what it does *not* do)
- Review and adapt it to your own environment
- Test it carefully in a non‑production setup

By using this script, **you accept full responsibility** for its effects.

⚠️ AI-assisted / vibe-coded experimental software. Use at your own risk.

## Disclaimer

This project is AI-assisted / vibe-coded software created as a hobby project. It has not been professionally audited and may contain bugs, unsafe behavior, data-loss issues, security problems, or incorrect assumptions.

You are responsible for reviewing the code, testing it in a safe environment, making backups, and understanding what it does before using it on real data. The author is not responsible for damage, data loss, broken systems, security issues, or other problems caused by using this software.

---

## Requirements

- Python 3.8 or newer.
- ZFS command-line tools available as `zfs`.
- Sanoid configuration directory containing `sanoid.conf`.
- Optional `sanoid.defaults.conf` in the same directory is read when present.
- Permission to list the datasets/snapshots being checked. Preflight checks for destroy-script generation also need permission to run `zfs holds`.

The Python program uses only the standard library; no pip packages are required.

This project has no separate application configuration file. Its own settings are the CLI arguments documented below; `--configdir` points to the external Sanoid configuration. The included `datasets_file` and `snapshot_ignore_file.example` are the project-specific input examples/templates.

## Dataset file

The required positional argument is a text file containing one ZFS dataset per line. Blank lines and lines beginning with `#` are ignored.

Example:

```text
# One exact ZFS dataset per line
Storage/Documents
Storage/Syncthing/Phone Backup
backup/host1
```

The included `datasets_file` is a commented example/template.

## Snapshot ignore-list file

`--ignore-list-file FILE` optionally loads one case-sensitive substring per nonblank, non-comment line. Matching is performed only against the snapshot name (the part after `@`), not against the dataset name. Patterns are literal substrings, not regular expressions or shell globs.

Ignored snapshots are removed from analysis before counting and classification. They therefore do **not** contribute to total snapshot counts, Sanoid counts, non-Sanoid lists, newest/stale calculations, off-schedule findings, cleanup candidates, JSON results, or generated destroy scripts.

Example:

```text
# Ignore snapshots created by SnapBeforeWatchTower
SnapBeforeWatchTower
```

That pattern ignores a snapshot named:

```text
SnapBeforeWatchTower-Date-2026-09-02_08_10_59
```

The included `snapshot_ignore_file.example` contains this example.

## Basic usage

```bash
python3 zfs-snapshots-checker.py DATASET_FILE --configdir /etc/sanoid
```

Example:

```bash
python3 zfs-snapshots-checker.py datasets_file --configdir /etc/sanoid
```

Show command help or the installed project version:

```bash
python3 zfs-snapshots-checker.py --help
python3 zfs-snapshots-checker.py --version
```

## What the checker reports

For each selected dataset, the program resolves the applicable Sanoid dataset section, including a recursive parent section when appropriate, then merges `template_default`, the selected `use_template`, and the dataset section. It lists snapshots with `zfs list` and reports relevant findings.

Current checks include:

- Stale or missing `hourly`, `daily`, `weekly`, `monthly`, and `yearly` Sanoid snapshots when `autosnap=yes` and that snapshot class has a configured count greater than zero.
- Sanoid snapshot counts above the configured desired count.
- Weekly/monthly Sanoid snapshots whose timestamp does not match the configured weekday/day, hour, and minute.
- Snapshots whose names do not match the Sanoid `autosnap_YYYY-MM-DD_HH:MM:SS_TYPE` naming pattern.
- Likely manual cleanup candidates, currently the same off-schedule weekly/monthly Sanoid snapshots identified by the schedule check.

The stale-age thresholds currently used by the code are 2 hours for hourly, 30 hours for daily, 8 days for weekly, 40 days for monthly, and 400 days for yearly snapshots.

## Arguments and flags

### `dataset_file`

Required positional argument. Path to the dataset list described above.

### `--configdir DIR`

Required. Directory containing `sanoid.conf`; `sanoid.defaults.conf` is loaded from the same directory when present.

```bash
--configdir /etc/sanoid
```

### `--json`

Print the complete analyzed result set as JSON instead of the normal text report. If `--write-destroy-script` is also used, the status line about the written script is printed before the JSON payload.

### `--show-ok`

Include datasets that have no findings. Without this flag, normal text output only prints datasets with findings or errors.

### `--only-stale`

Show datasets with stale or missing snapshots relevant to active `autosnap=yes` policies, plus dataset errors.

### `--only-exceeds`

Show datasets whose Sanoid snapshot counts exceed desired values, plus dataset errors.

### `--only-non-sanoid`

Show datasets containing snapshots that do not match the Sanoid naming format, plus dataset errors.

When `--write-destroy-script` is also used **without** an explicit `--write-destroy-script-mode`, this flag also makes the destroy-script mode default to `non-sanoid`. This means the natural command `--write-destroy-script FILE --only-non-sanoid` writes only non-Sanoid snapshots, not Sanoid cleanup candidates.

### `--only-offsched`

Show datasets containing weekly or monthly Sanoid snapshots outside the configured schedule, plus dataset errors.

### `--only-cleanup-candidates`

Show datasets with likely manual cleanup candidates, plus dataset errors.

Use one `--only-*` report filter at a time. The current implementation checks them in a fixed precedence order rather than combining multiple filter types.

### `--only-dataset DATASET`

Analyze only one exact dataset name from the supplied dataset file. The name must already exist in that input file.

```bash
python3 zfs-snapshots-checker.py datasets_file \
  --configdir /etc/sanoid \
  --only-dataset "Storage/Syncthing/Phone Backup"
```

### `--recursively`

Use only together with `--only-dataset`. It selects the requested dataset plus every descendant dataset already present in the supplied `dataset_file`. The checker deliberately does not discover extra ZFS child datasets outside that file, so the dataset file remains an explicit allow-list.

Example:

```bash
python3 zfs-snapshots-checker.py datasets_file \
  --configdir /etc/sanoid \
  --only-dataset "Storage/Syncthing" \
  --recursively
```

If the dataset file contains `Storage/Syncthing`, `Storage/Syncthing/Phone`, and `Storage/Syncthing/PC`, all three are analyzed. `--recursively` without `--only-dataset` is rejected.

### `--ignore-list-file FILE`

Load snapshot-name substrings to ignore, using the format described in [Snapshot ignore-list file](#snapshot-ignore-list-file). Matching is case-sensitive and occurs before counts, findings, JSON serialization, and destroy-script candidate selection.

Example:

```bash
python3 zfs-snapshots-checker.py datasets_file \
  --configdir /etc/sanoid \
  --ignore-list-file snapshot_ignore_file.example
```

Write only non-Sanoid snapshots while applying an ignore list:

```bash
python3 zfs-snapshots-checker.py datasets \
  --configdir /etc/sanoid \
  --ignore-list-file ignore-list \
  --write-destroy-script candidates \
  --only-non-sanoid
```

In this combination, `--only-non-sanoid` makes the implicit destroy-script mode `non-sanoid`, so off-schedule weekly/monthly Sanoid cleanup candidates are not written.

### `--write-destroy-script FILE`

Write selected `zfs destroy` commands to a shell script. The checker never executes that script. Review every generated command before running anything.

### `--write-destroy-script-mode {candidates,non-sanoid,both}`

Select what `--write-destroy-script` writes:

- `candidates` — likely cleanup candidates (off-schedule weekly/monthly Sanoid snapshots).
- `non-sanoid` — snapshots that do not match the Sanoid naming format.
- `both` — both groups.

Default behavior:

- With `--only-non-sanoid`, the implicit mode is `non-sanoid`.
- Otherwise, the implicit mode is `candidates`.
- An explicitly supplied `--write-destroy-script-mode` always takes precedence.

Any explicit `--write-destroy-script-mode` is rejected unless `--write-destroy-script` is also supplied.

### `--append-destroy-script`

When writing a destroy script, append to the existing file rather than replacing it. Without this flag, the generated script is overwritten and made executable (`0755`).

### `--dry-run-destroy-check`

When writing a destroy script, preflight each selected snapshot before leaving an active `zfs destroy` line. The program checks that the snapshot exists and queries `zfs holds`.

Entries are commented out when the snapshot is missing, has holds, or cannot be checked reliably. Passing this flag still does **not** execute any destroy command.

### `--version`

Print the application version and exit.

### `-h`, `--help`

Print built-in command help and exit.

## Recommended safe workflow

1. Inspect a single dataset first, or add `--recursively` if you intentionally want its listed descendants too:

```bash
python3 zfs-snapshots-checker.py datasets_file \
  --configdir /etc/sanoid \
  --only-dataset "Storage/Documents" \
  --show-ok
```

2. If you use intentionally named snapshots that should never participate in the audit/cleanup logic, create and review an ignore list, then add for example:

```bash
--ignore-list-file snapshot_ignore_file.example
```

3. Generate a preflight-checked cleanup script:

```bash
python3 zfs-snapshots-checker.py datasets_file \
  --configdir /etc/sanoid \
  --only-dataset "Storage/Documents" \
  --write-destroy-script cleanup.sh \
  --write-destroy-script-mode both \
  --dry-run-destroy-check
```

4. Review the generated file carefully:

```bash
cat cleanup.sh
```

5. Independently verify each snapshot and any ZFS holds. If you decide to delete anything, run individual commands manually first rather than blindly executing a generated batch.

## Output examples

Normal text output includes the resolved policy, current and desired counts, newest snapshots, and any detected findings.

Machine-readable output:

```bash
python3 zfs-snapshots-checker.py datasets_file --configdir /etc/sanoid --json
```

Only datasets with non-Sanoid snapshots:

```bash
python3 zfs-snapshots-checker.py datasets_file \
  --configdir /etc/sanoid \
  --only-non-sanoid
```

## Sanoid policy resolution

For each input dataset, policy resolution works in this order:

1. Use an exact dataset section from `sanoid.conf` when present.
2. Otherwise walk parent datasets upward and use the nearest section whose `recursive` value is true.
3. Merge `template_default` values.
4. Merge the selected `use_template` values, if that template exists.
5. Merge the dataset/recursive section last so its values take precedence.

If no applicable section is found, snapshot listing still runs, but policy-dependent checks have no matching policy to use.

## Exit behavior

- `0` — normal completion, including when no datasets match the selected report filter.
- `1` — startup/input validation failure such as a missing dataset file, missing Sanoid directory/config, a missing ignore-list file, an empty dataset file, an `--only-dataset` value that is not present in the input list, or `--recursively` used without `--only-dataset`.

A per-dataset `zfs list` failure is recorded in that dataset's result and does not currently change the program's final exit status.

## Destructive-operation warning

The application itself audits and generates commands; it does not delete snapshots automatically. However, any generated shell script can contain real `zfs destroy` commands. Running those commands can permanently delete snapshots and data that depend on them.

Always keep independent backups and review the generated script manually before executing any destructive command.
