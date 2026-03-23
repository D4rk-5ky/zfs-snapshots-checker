#!/usr/bin/env python3

import argparse
import configparser
import json
import re
import shlex
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any


SANOID_REGEX = re.compile(
    r"^autosnap_(\d{4}-\d{2}-\d{2})_(\d{2}:\d{2}:\d{2})_(frequently|hourly|daily|weekly|monthly|yearly)$"
)

SNAP_TYPES = ("frequently", "hourly", "daily", "weekly", "monthly", "yearly")


@dataclass
class SnapshotInfo:
    full_name: str
    dataset: str
    snap_name: str
    is_sanoid: bool
    snap_type: Optional[str] = None
    timestamp: Optional[datetime] = None


@dataclass
class DatasetPolicy:
    dataset: str
    source_section: Optional[str] = None
    recursive_source: Optional[str] = None
    use_template: Optional[str] = None
    autosnap: Optional[bool] = None
    autoprune: Optional[bool] = None
    recursive: Optional[bool] = None
    counts_desired: Dict[str, int] = field(default_factory=dict)
    schedule: Dict[str, Any] = field(default_factory=dict)
    inherited_from: Optional[str] = None
    merged_values: Dict[str, str] = field(default_factory=dict)


@dataclass
class DatasetResult:
    dataset: str
    error: Optional[str] = None
    policy: Optional[DatasetPolicy] = None
    total_snapshots: int = 0
    sanoid_counts: Dict[str, int] = field(default_factory=dict)
    non_sanoid_snapshots: List[str] = field(default_factory=list)
    offschedule_snapshots: List[str] = field(default_factory=list)
    newest_by_type: Dict[str, Optional[SnapshotInfo]] = field(default_factory=dict)
    stale_reasons: List[str] = field(default_factory=list)
    exceeds_reasons: List[str] = field(default_factory=list)
    likely_manual_cleanup_candidates: List[str] = field(default_factory=list)


def run_command(cmd: List[str]) -> Tuple[int, str, str]:
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
        )
        return proc.returncode, proc.stdout, proc.stderr
    except Exception as exc:
        return 1, "", str(exc)


def parse_bool(value: Optional[str]) -> Optional[bool]:
    if value is None:
        return None
    v = value.strip().lower()
    if v in ("yes", "true", "1", "on"):
        return True
    if v in ("no", "false", "0", "off"):
        return False
    return None


def parse_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except Exception:
        return None


def read_dataset_file(path: Path) -> List[str]:
    datasets: List[str] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            datasets.append(line)
    return datasets


def load_ini(path: Path) -> configparser.ConfigParser:
    cfg = configparser.ConfigParser(interpolation=None, strict=False)
    cfg.optionxform = str
    with path.open("r", encoding="utf-8") as fh:
        cfg.read_file(fh)
    return cfg


def extract_template_name(section_name: str) -> Optional[str]:
    if section_name.startswith("template_"):
        return section_name[len("template_"):]
    return None


def build_maps(
    sanoid_cfg: configparser.ConfigParser,
    defaults_cfg: Optional[configparser.ConfigParser],
) -> Tuple[Dict[str, Dict[str, str]], Dict[str, Dict[str, str]], Dict[str, str]]:
    templates: Dict[str, Dict[str, str]] = {}
    dataset_sections: Dict[str, Dict[str, str]] = {}
    default_template: Dict[str, str] = {}

    if defaults_cfg is not None:
        for section in defaults_cfg.sections():
            if section == "template_default":
                default_template = dict(defaults_cfg.items(section))
            else:
                tname = extract_template_name(section)
                if tname:
                    templates[tname] = dict(defaults_cfg.items(section))

    for section in sanoid_cfg.sections():
        if section == "template_default":
            default_template.update(dict(sanoid_cfg.items(section)))
            continue

        tname = extract_template_name(section)
        if tname:
            current = templates.get(tname, {}).copy()
            current.update(dict(sanoid_cfg.items(section)))
            templates[tname] = current
        else:
            dataset_sections[section] = dict(sanoid_cfg.items(section))

    return templates, dataset_sections, default_template


def parent_datasets(dataset: str) -> List[str]:
    parts = dataset.split("/")
    return ["/".join(parts[:i]) for i in range(len(parts), 0, -1)]


def resolve_policy(
    dataset: str,
    dataset_sections: Dict[str, Dict[str, str]],
    templates: Dict[str, Dict[str, str]],
    default_template: Dict[str, str],
) -> DatasetPolicy:
    matched_section: Optional[str] = None
    matched_recursive: Optional[str] = None

    if dataset in dataset_sections:
        matched_section = dataset

    if matched_section is None:
        for parent in parent_datasets(dataset)[1:]:
            opts = dataset_sections.get(parent)
            if not opts:
                continue
            if parse_bool(opts.get("recursive")):
                matched_recursive = parent
                break

    source_section = matched_section or matched_recursive
    policy = DatasetPolicy(
        dataset=dataset,
        source_section=source_section,
        recursive_source=matched_recursive,
    )

    if source_section is None:
        return policy

    section_opts = dataset_sections[source_section]
    use_template = section_opts.get("use_template")
    policy.use_template = use_template
    policy.inherited_from = source_section if source_section != dataset else None

    merged: Dict[str, str] = {}
    merged.update(default_template)

    if use_template and use_template in templates:
        merged.update(templates[use_template])

    merged.update(section_opts)

    policy.merged_values = merged
    policy.autosnap = parse_bool(merged.get("autosnap"))
    policy.autoprune = parse_bool(merged.get("autoprune"))
    policy.recursive = parse_bool(merged.get("recursive"))

    for snap_type in SNAP_TYPES:
        policy.counts_desired[snap_type] = parse_int(merged.get(snap_type)) or 0

    schedule_keys = (
        "hourly_min",
        "daily_hour", "daily_min",
        "weekly_wday", "weekly_hour", "weekly_min",
        "monthly_mday", "monthly_hour", "monthly_min",
        "yearly_mon", "yearly_mday", "yearly_hour", "yearly_min",
        "frequent_period",
    )
    for key in schedule_keys:
        val = merged.get(key)
        iv = parse_int(val)
        policy.schedule[key] = iv if iv is not None else val

    return policy


def list_snapshots_for_dataset(dataset: str) -> Tuple[List[str], Optional[str]]:
    cmd = ["zfs", "list", "-H", "-t", "snapshot", "-o", "name", "-r", dataset]
    rc, stdout, stderr = run_command(cmd)

    if rc != 0:
        return [], stderr.strip() or f"zfs list failed for dataset: {dataset}"

    snapshots: List[str] = []
    for line in stdout.splitlines():
        line = line.strip()
        if not line or "@" not in line:
            continue
        ds, _ = line.split("@", 1)
        if ds == dataset:
            snapshots.append(line)

    return snapshots, None


def parse_snapshot(snapshot_full_name: str) -> SnapshotInfo:
    dataset, snap_name = snapshot_full_name.split("@", 1)
    match = SANOID_REGEX.match(snap_name)
    if not match:
        return SnapshotInfo(
            full_name=snapshot_full_name,
            dataset=dataset,
            snap_name=snap_name,
            is_sanoid=False,
        )

    snap_type = match.group(3)
    ts = None
    try:
        ts = datetime.strptime(f"{match.group(1)} {match.group(2)}", "%Y-%m-%d %H:%M:%S")
    except Exception:
        pass

    return SnapshotInfo(
        full_name=snapshot_full_name,
        dataset=dataset,
        snap_name=snap_name,
        is_sanoid=True,
        snap_type=snap_type,
        timestamp=ts,
    )


def newest_snapshot_by_type(snapshots: List[SnapshotInfo]) -> Dict[str, Optional[SnapshotInfo]]:
    newest: Dict[str, Optional[SnapshotInfo]] = {snap_type: None for snap_type in SNAP_TYPES}
    for snap in snapshots:
        if not snap.is_sanoid or not snap.snap_type or snap.timestamp is None:
            continue
        current = newest.get(snap.snap_type)
        if current is None or (current.timestamp and snap.timestamp > current.timestamp):
            newest[snap.snap_type] = snap
    return newest


def format_timedelta(delta: timedelta) -> str:
    total_seconds = int(delta.total_seconds())
    if total_seconds < 0:
        total_seconds = 0

    days, rem = divmod(total_seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, _ = divmod(rem, 60)

    parts: List[str] = []
    if days:
        parts.append(f"{days}d")
    if hours or days:
        parts.append(f"{hours}h")
    parts.append(f"{minutes}m")
    return " ".join(parts)


def stale_thresholds_from_policy(policy: DatasetPolicy) -> Dict[str, timedelta]:
    thresholds: Dict[str, timedelta] = {}
    if policy.counts_desired.get("hourly", 0) > 0:
        thresholds["hourly"] = timedelta(hours=2)
    if policy.counts_desired.get("daily", 0) > 0:
        thresholds["daily"] = timedelta(hours=30)
    if policy.counts_desired.get("weekly", 0) > 0:
        thresholds["weekly"] = timedelta(days=8)
    if policy.counts_desired.get("monthly", 0) > 0:
        thresholds["monthly"] = timedelta(days=40)
    if policy.counts_desired.get("yearly", 0) > 0:
        thresholds["yearly"] = timedelta(days=400)
    return thresholds


def find_stale_autosnap_reasons(
    policy: Optional[DatasetPolicy],
    newest: Dict[str, Optional[SnapshotInfo]],
    now: datetime,
) -> List[str]:
    reasons: List[str] = []
    if policy is None or policy.autosnap is not True:
        return reasons

    thresholds = stale_thresholds_from_policy(policy)

    for snap_type, threshold in thresholds.items():
        desired = policy.counts_desired.get(snap_type, 0)
        if desired <= 0:
            continue

        newest_snap = newest.get(snap_type)
        if newest_snap is None or newest_snap.timestamp is None:
            reasons.append(f"missing {snap_type} snapshots")
            continue

        age = now - newest_snap.timestamp
        if age > threshold:
            reasons.append(
                f"{snap_type} newest snapshot too old: {newest_snap.snap_name} ({format_timedelta(age)})"
            )

    return reasons


def find_exceeds(policy: Optional[DatasetPolicy], counts: Dict[str, int]) -> List[str]:
    reasons: List[str] = []
    if policy is None:
        return reasons

    for snap_type in ("hourly", "daily", "weekly", "monthly", "yearly"):
        desired = policy.counts_desired.get(snap_type, 0)
        current = counts.get(snap_type, 0)
        if desired >= 0 and current > desired:
            reasons.append(f"{snap_type}={current} > {desired}")
    return reasons


def is_offschedule(snapshot: SnapshotInfo, policy: Optional[DatasetPolicy]) -> bool:
    if not snapshot.is_sanoid or snapshot.timestamp is None or policy is None or snapshot.snap_type is None:
        return False

    ts = snapshot.timestamp
    sched = policy.schedule

    if snapshot.snap_type == "weekly":
        wday = sched.get("weekly_wday")
        hour = sched.get("weekly_hour")
        minute = sched.get("weekly_min")
        if wday is None or hour is None or minute is None:
            return False
        return not ((ts.weekday() + 1) == wday and ts.hour == hour and ts.minute == minute)

    if snapshot.snap_type == "monthly":
        mday = sched.get("monthly_mday")
        hour = sched.get("monthly_hour")
        minute = sched.get("monthly_min")
        if mday is None or hour is None or minute is None:
            return False
        return not (ts.day == mday and ts.hour == hour and ts.minute == minute)

    return False


def find_likely_manual_cleanup_candidates(
    policy: Optional[DatasetPolicy],
    snapshots: List[SnapshotInfo],
) -> List[str]:
    candidates: List[str] = []
    if policy is None:
        return candidates

    for snap in snapshots:
        if not snap.is_sanoid or snap.snap_type not in ("weekly", "monthly") or snap.timestamp is None:
            continue
        if is_offschedule(snap, policy):
            candidates.append(snap.snap_name)

    return sorted(set(candidates))


def snapshot_exists(full_snapshot_name: str) -> Tuple[bool, Optional[str]]:
    cmd = ["zfs", "list", "-H", "-t", "snapshot", "-o", "name", full_snapshot_name]
    rc, stdout, stderr = run_command(cmd)

    if rc == 0:
        lines = [line.strip() for line in stdout.splitlines() if line.strip()]
        return full_snapshot_name in lines, None

    err = stderr.strip() or stdout.strip()
    if "does not exist" in err.lower() or "dataset does not exist" in err.lower():
        return False, None

    return False, err or "unknown error checking snapshot existence"


def snapshot_has_holds(full_snapshot_name: str) -> Tuple[Optional[bool], List[str], Optional[str]]:
    cmd = ["zfs", "holds", full_snapshot_name]
    rc, stdout, stderr = run_command(cmd)

    if rc != 0:
        err = stderr.strip() or stdout.strip() or "unknown error checking holds"
        return None, [], err

    tags: List[str] = []
    for line in stdout.splitlines():
        line = line.strip()
        if not line or line.startswith("NAME"):
            continue
        parts = line.split()
        if len(parts) >= 2 and parts[0] == full_snapshot_name:
            tags.append(parts[1])

    return len(tags) > 0, tags, None


def analyze_dataset(
    dataset: str,
    policy: Optional[DatasetPolicy],
    now: datetime,
) -> DatasetResult:
    result = DatasetResult(
        dataset=dataset,
        policy=policy,
        sanoid_counts={snap_type: 0 for snap_type in SNAP_TYPES},
        newest_by_type={snap_type: None for snap_type in SNAP_TYPES},
    )

    snapshots_raw, error = list_snapshots_for_dataset(dataset)
    if error:
        result.error = error
        return result

    parsed = [parse_snapshot(s) for s in snapshots_raw]
    result.total_snapshots = len(parsed)

    for snap in parsed:
        if snap.is_sanoid and snap.snap_type:
            result.sanoid_counts[snap.snap_type] += 1
            if is_offschedule(snap, policy) and snap.snap_type in ("weekly", "monthly"):
                result.offschedule_snapshots.append(snap.snap_name)
        else:
            result.non_sanoid_snapshots.append(snap.snap_name)

    result.newest_by_type = newest_snapshot_by_type(parsed)
    result.stale_reasons = find_stale_autosnap_reasons(policy, result.newest_by_type, now)
    result.exceeds_reasons = find_exceeds(policy, result.sanoid_counts)
    result.likely_manual_cleanup_candidates = find_likely_manual_cleanup_candidates(policy, parsed)

    return result


def serialize_result(result: DatasetResult) -> Dict[str, Any]:
    def snap_to_dict(s: Optional[SnapshotInfo]) -> Optional[Dict[str, Any]]:
        if s is None:
            return None
        return {
            "snap_name": s.snap_name,
            "timestamp": s.timestamp.isoformat(sep=" ") if s.timestamp else None,
            "snap_type": s.snap_type,
        }

    return {
        "dataset": result.dataset,
        "error": result.error,
        "policy": {
            "source_section": result.policy.source_section if result.policy else None,
            "recursive_source": result.policy.recursive_source if result.policy else None,
            "use_template": result.policy.use_template if result.policy else None,
            "autosnap": result.policy.autosnap if result.policy else None,
            "autoprune": result.policy.autoprune if result.policy else None,
            "counts_desired": result.policy.counts_desired if result.policy else None,
        } if result.policy else None,
        "total_snapshots": result.total_snapshots,
        "sanoid_counts": result.sanoid_counts,
        "non_sanoid_snapshots": result.non_sanoid_snapshots,
        "offschedule_snapshots": result.offschedule_snapshots,
        "likely_manual_cleanup_candidates": result.likely_manual_cleanup_candidates,
        "stale_reasons": result.stale_reasons,
        "exceeds_reasons": result.exceeds_reasons,
        "newest_by_type": {
            k: snap_to_dict(v) for k, v in result.newest_by_type.items()
        },
    }


def should_print(
    result: DatasetResult,
    only_stale: bool,
    only_exceeds: bool,
    only_nonsanoid: bool,
    only_offschedule: bool,
    only_cleanup_candidates: bool,
    show_ok: bool,
) -> bool:
    has_stale = bool(result.stale_reasons)
    has_exceeds = bool(result.exceeds_reasons)
    has_nonsanoid = bool(result.non_sanoid_snapshots)
    has_offschedule = bool(result.offschedule_snapshots)
    has_cleanup_candidates = bool(result.likely_manual_cleanup_candidates)
    has_error = bool(result.error)

    if only_stale:
        return has_stale or has_error
    if only_exceeds:
        return has_exceeds or has_error
    if only_nonsanoid:
        return has_nonsanoid or has_error
    if only_offschedule:
        return has_offschedule or has_error
    if only_cleanup_candidates:
        return has_cleanup_candidates or has_error

    if show_ok:
        return True

    return (
        has_stale
        or has_exceeds
        or has_nonsanoid
        or has_offschedule
        or has_cleanup_candidates
        or has_error
    )


def print_result(result: DatasetResult) -> None:
    print(f"DATASET: {result.dataset}")

    if result.error:
        print(f"  ERROR: {result.error}")
        print()
        return

    policy = result.policy
    if policy is None or policy.source_section is None:
        print("  Policy: no matching sanoid section found")
    else:
        mode = "autosnap" if policy.autosnap else "cleanup-only"
        inherited = f" (inherited from {policy.inherited_from})" if policy.inherited_from else ""
        print(
            f"  Policy: template={policy.use_template or 'none'} mode={mode} section={policy.source_section}{inherited}"
        )

    print(f"  Total snapshots: {result.total_snapshots}")
    print(
        "  Counts: "
        f"hourly={result.sanoid_counts.get('hourly', 0)} "
        f"daily={result.sanoid_counts.get('daily', 0)} "
        f"weekly={result.sanoid_counts.get('weekly', 0)} "
        f"monthly={result.sanoid_counts.get('monthly', 0)} "
        f"yearly={result.sanoid_counts.get('yearly', 0)}"
    )

    if policy:
        print(
            "  Desired: "
            f"hourly={policy.counts_desired.get('hourly', 0)} "
            f"daily={policy.counts_desired.get('daily', 0)} "
            f"weekly={policy.counts_desired.get('weekly', 0)} "
            f"monthly={policy.counts_desired.get('monthly', 0)} "
            f"yearly={policy.counts_desired.get('yearly', 0)}"
        )

    for snap_type in ("hourly", "daily", "weekly", "monthly", "yearly"):
        newest = result.newest_by_type.get(snap_type)
        if newest and newest.timestamp:
            print(f"  Newest {snap_type}: {newest.snap_name}")

    if result.stale_reasons:
        print("  Stale autosnap issues:")
        for reason in result.stale_reasons:
            print(f"    - {reason}")

    if result.exceeds_reasons:
        print("  Exceeds desired counts:")
        for reason in result.exceeds_reasons:
            print(f"    - {reason}")

    if result.offschedule_snapshots:
        print("  Off-schedule sanoid snapshots:")
        for snap in result.offschedule_snapshots:
            print(f"    - {snap}")

    if result.likely_manual_cleanup_candidates:
        print("  Likely manual cleanup candidates:")
        for snap in result.likely_manual_cleanup_candidates:
            print(f"    - {snap}")

    if result.non_sanoid_snapshots:
        print("  Non-Sanoid snapshots:")
        for snap in result.non_sanoid_snapshots:
            print(f"    - {snap}")

    if not (
        result.stale_reasons
        or result.exceeds_reasons
        or result.offschedule_snapshots
        or result.likely_manual_cleanup_candidates
        or result.non_sanoid_snapshots
    ):
        print("  OK")

    print()


def write_destroy_script(
    output_path: Path,
    results: List[DatasetResult],
    mode: str = "candidates",
    append: bool = False,
    dry_run_check: bool = False,
) -> int:
    lines: List[str] = []

    if not append or not output_path.exists():
        lines.append("#!/usr/bin/env bash")
        lines.append("set -euo pipefail")
        lines.append("")
        lines.append("# Review carefully before running.")
        lines.append("# Generated by zfs_sanoid_check_v2_4.py")
        lines.append(f"# Mode: {mode}")
        lines.append(f"# Dry-run preflight checks: {'enabled' if dry_run_check else 'disabled'}")
        lines.append("")

    total_commands = 0

    for result in results:
        if result.error:
            continue

        if mode == "candidates":
            selected = list(result.likely_manual_cleanup_candidates)
        elif mode == "non-sanoid":
            selected = list(result.non_sanoid_snapshots)
        elif mode == "both":
            selected = list(result.likely_manual_cleanup_candidates) + list(result.non_sanoid_snapshots)
        else:
            raise ValueError(f"Unsupported destroy script mode: {mode}")

        deduped = sorted(set(selected))
        if not deduped:
            continue

        lines.append(f"# Dataset: {result.dataset}")

        for snap_name in deduped:
            full_snap = f"{result.dataset}@{snap_name}"
            quoted = shlex.quote(full_snap)

            if not dry_run_check:
                lines.append(f"zfs destroy {quoted}")
                total_commands += 1
                continue

            exists, exists_err = snapshot_exists(full_snap)
            if exists_err:
                lines.append(f"# ERROR checking snapshot: {full_snap}")
                lines.append(f"# {exists_err}")
                lines.append(f"# zfs destroy {quoted}")
                continue

            if not exists:
                lines.append(f"# NOT FOUND: {full_snap}")
                lines.append(f"# zfs destroy {quoted}")
                continue

            has_holds, hold_tags, holds_err = snapshot_has_holds(full_snap)
            if holds_err:
                lines.append(f"# ERROR checking holds: {full_snap}")
                lines.append(f"# {holds_err}")
                lines.append(f"# zfs destroy {quoted}")
                continue

            if has_holds is True:
                tag_text = ", ".join(hold_tags) if hold_tags else "unknown"
                lines.append(f"# HELD: {full_snap}")
                lines.append(f"# Hold tags: {tag_text}")
                lines.append(f"# zfs destroy {quoted}")
                continue

            lines.append(f"# OK: {full_snap}")
            lines.append(f"zfs destroy {quoted}")
            total_commands += 1

        lines.append("")

    file_mode = "a" if append else "w"
    with output_path.open(file_mode, encoding="utf-8") as fh:
        fh.write("\n".join(lines).rstrip() + "\n")

    if not append:
        output_path.chmod(0o755)

    return total_commands


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="ZFS + Sanoid aware snapshot checker (v2.4)"
    )
    parser.add_argument(
        "dataset_file",
        help="Text file with datasets, one per line",
    )
    parser.add_argument(
        "--configdir",
        required=True,
        help="Path to Sanoid config directory, e.g. /etc/sanoid or /etc/sanoid-RPI5-Storage",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output JSON instead of normal text output",
    )
    parser.add_argument(
        "--show-ok",
        action="store_true",
        help="Also show datasets that have no findings",
    )
    parser.add_argument(
        "--only-stale",
        action="store_true",
        help="Show only autosnap datasets with stale or missing snapshots",
    )
    parser.add_argument(
        "--only-exceeds",
        action="store_true",
        help="Show only datasets where Sanoid snapshot counts exceed desired values",
    )
    parser.add_argument(
        "--only-non-sanoid",
        action="store_true",
        help="Show only datasets containing non-Sanoid snapshots",
    )
    parser.add_argument(
        "--only-offsched",
        action="store_true",
        help="Show only datasets containing off-schedule weekly or monthly Sanoid snapshots",
    )
    parser.add_argument(
        "--only-cleanup-candidates",
        action="store_true",
        help="Show only datasets with likely manual cleanup candidates",
    )
    parser.add_argument(
        "--only-dataset",
        help="Only analyze one exact dataset name from the dataset file",
    )
    parser.add_argument(
        "--write-destroy-script",
        help="Write selected destroy commands to a shell script",
    )
    parser.add_argument(
        "--write-destroy-script-mode",
        choices=("candidates", "non-sanoid", "both"),
        default="candidates",
        help="Choose what goes into the destroy script: likely cleanup candidates, non-Sanoid snapshots, or both",
    )
    parser.add_argument(
        "--append-destroy-script",
        action="store_true",
        help="Append to an existing destroy script instead of overwriting it",
    )
    parser.add_argument(
        "--dry-run-destroy-check",
        action="store_true",
        help="Preflight-check destroy-script snapshots for existence and holds, and comment out unsafe entries",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()

    dataset_file = Path(args.dataset_file)
    configdir = Path(args.configdir)
    sanoid_conf = configdir / "sanoid.conf"
    sanoid_defaults = configdir / "sanoid.defaults.conf"

    if not dataset_file.exists():
        print(f"ERROR: dataset file does not exist: {dataset_file}", file=sys.stderr)
        return 1

    if not configdir.exists():
        print(f"ERROR: configdir does not exist: {configdir}", file=sys.stderr)
        return 1

    if not sanoid_conf.exists():
        print(f"ERROR: missing {sanoid_conf}", file=sys.stderr)
        return 1

    if args.write_destroy_script_mode != "candidates" and not args.write_destroy_script:
        print(
            "ERROR: --write-destroy-script-mode requires --write-destroy-script",
            file=sys.stderr,
        )
        return 1

    defaults_cfg: Optional[configparser.ConfigParser] = None
    if sanoid_defaults.exists():
        defaults_cfg = load_ini(sanoid_defaults)

    sanoid_cfg = load_ini(sanoid_conf)

    datasets = read_dataset_file(dataset_file)
    if not datasets:
        print("ERROR: no datasets found in input file", file=sys.stderr)
        return 1

    if args.only_dataset:
        datasets = [d for d in datasets if d == args.only_dataset]
        if not datasets:
            print(f'ERROR: dataset "{args.only_dataset}" not found in dataset file', file=sys.stderr)
            return 1

    templates, dataset_sections, default_template = build_maps(sanoid_cfg, defaults_cfg)
    now = datetime.now()

    results: List[DatasetResult] = []

    for dataset in datasets:
        policy = resolve_policy(dataset, dataset_sections, templates, default_template)
        result = analyze_dataset(dataset, policy, now)
        results.append(result)

    if args.write_destroy_script:
        output_path = Path(args.write_destroy_script)
        count = write_destroy_script(
            output_path=output_path,
            results=results,
            mode=args.write_destroy_script_mode,
            append=args.append_destroy_script,
            dry_run_check=args.dry_run_destroy_check,
        )
        print(
            f"Wrote destroy script: {output_path} "
            f"(mode={args.write_destroy_script_mode}, "
            f"dry_run_check={'on' if args.dry_run_destroy_check else 'off'}, "
            f"{count} active destroy commands)"
        )

    if args.json:
        print(json.dumps([serialize_result(r) for r in results], indent=2))
        return 0

    any_output = False
    for result in results:
        if should_print(
            result=result,
            only_stale=args.only_stale,
            only_exceeds=args.only_exceeds,
            only_nonsanoid=args.only_non_sanoid,
            only_offschedule=args.only_offsched,
            only_cleanup_candidates=args.only_cleanup_candidates,
            show_ok=args.show_ok,
        ):
            print_result(result)
            any_output = True

    if not any_output:
        print("No datasets matched the selected filters.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
