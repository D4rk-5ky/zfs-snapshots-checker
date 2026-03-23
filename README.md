# ZFS Sanoid Check Script -- README

## ⚠️ Disclaimer

This tool **generates ZFS destroy commands**.

You are **fully responsible** for: - Reviewing all output - Verifying
snapshots - Executing any generated scripts

❗ **Incorrect use can permanently destroy data.**

Always: - Test on a single dataset first - Review scripts manually -
Never blindly execute generated commands

------------------------------------------------------------------------

## 📦 Usage

``` bash
python3 zfs_sanoid_check_v2_4.py DATASET_FILE --configdir /etc/sanoid
```

------------------------------------------------------------------------

## 📁 Arguments

### `dataset_file`

Text file with datasets (one per line)

------------------------------------------------------------------------

## ⚙️ Required Flags

### `--configdir`

Path to Sanoid config directory

Example:

``` bash
--configdir /etc/sanoid
```

------------------------------------------------------------------------

## 📊 Output Flags

### `--json`

Machine-readable JSON output

### `--show-ok`

Include datasets with no issues

------------------------------------------------------------------------

## 🔍 Filtering Flags

### `--only-stale`

Show stale datasets (autosnap=yes only)

### `--only-exceeds`

Show datasets exceeding retention policy

### `--only-non-sanoid`

Show non-Sanoid snapshots (syncoid, manual, etc.)

### `--only-offsched`

Show snapshots outside expected schedule

### `--only-cleanup-candidates`

Show likely manual cleanup candidates

### `--only-dataset`

Limit to one dataset

Example:

``` bash
--only-dataset "Storage/Syncthing/Phone Backup"
```

------------------------------------------------------------------------

## 💣 Destroy Script Flags

### `--write-destroy-script FILE`

Write `zfs destroy` commands to file

### `--write-destroy-script-mode`

Options: - `candidates` → off-schedule Sanoid snapshots - `non-sanoid` →
non-Sanoid snapshots - `both` → everything

### `--append-destroy-script`

Append instead of overwrite

### `--dry-run-destroy-check`

Preflight check before writing destroy commands

Checks: - Snapshot exists - Snapshot has holds

------------------------------------------------------------------------

## ✅ Recommended Safe Workflow

``` bash
# 1. Inspect dataset
python3 script.py datasets.txt --configdir /etc/sanoid --only-dataset "DATASET"

# 2. Generate safe cleanup script
python3 script.py datasets.txt   --configdir /etc/sanoid   --only-dataset "DATASET"   --write-destroy-script cleanup.sh   --write-destroy-script-mode both   --dry-run-destroy-check

# 3. Review script
cat cleanup.sh

# 4. Run ONE command manually first

# 5. Execute script only when confident
```

------------------------------------------------------------------------

## 🧠 Notes

-   Works with `autosnap=no` (cleanup-only datasets)
-   Detects:
    -   Over-retention
    -   Off-schedule snapshots
    -   Non-Sanoid snapshots
-   Safe-by-design (no automatic deletion)

------------------------------------------------------------------------

## 🚨 Final Responsibility

This script **does NOT delete anything automatically**.

But:

👉 You are responsible for any command you execute.

Always double-check before running:

``` bash
zfs destroy ...
```

