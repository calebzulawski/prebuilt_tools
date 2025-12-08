#!/usr/bin/env python3
"""
Materialize per-version conda environments for a set of tools defined in
config.json. The script queries conda-forge for available versions and writes
minimal environment files that lock to each minor version.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import shutil
import sys
import textwrap
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Set, Tuple

from rattler.channel import Channel
from rattler.exceptions import SolverError
from rattler.platform import Platform
from rattler.repo_data.gateway import Gateway
from rattler.solver.solver import solve as solve_environment


DEFAULT_CHANNEL_URL = "https://conda.anaconda.org/conda-forge"
SKIP_PLATFORMS = {"win-32"}
DEFAULT_SUBDIRS = tuple(
    str(platform) for platform in Platform.all() if str(platform) not in SKIP_PLATFORMS
)
MINOR_VERSION_PATTERN = re.compile(r"^(?P<major>\d+)\.(?P<minor>\d+)")


@dataclass(frozen=True)
class PackageSpec:
    package: str
    executables: Tuple[str, ...]
    win_package: str | None = None
    min_minor_version: Tuple[int, int] | None = None


def _normalize_executables(raw: Iterable[str] | str | None, fallback: str) -> Tuple[str, ...]:
    if raw is None:
        candidates = [fallback]
    elif isinstance(raw, str):
        candidates = [raw]
    else:
        candidates = list(raw)

    normalized = tuple(
        value.strip()
        for value in candidates
        if isinstance(value, str) and value.strip()
    )
    if not normalized:
        raise ValueError("Each package entry must define at least one executable name")
    return normalized


def read_mapping(path: Path) -> List[PackageSpec]:
    data = json.loads(path.read_text())
    if isinstance(data, dict):
        entries = [
            {
                "package": package_name,
                "executables": [binary_name],
            }
            for binary_name, package_name in data.items()
        ]
    elif isinstance(data, list):
        entries = data
    else:
        raise ValueError("config.json must be an array or an object mapping binaries to packages")

    specs: List[PackageSpec] = []
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError("Each package entry must be an object")
        package_name = entry.get("package")
        if not isinstance(package_name, str) or not package_name.strip():
            raise ValueError("Each package entry must include a non-empty 'package'")
        package_name = package_name.strip()
        executables = _normalize_executables(entry.get("executables"), package_name)
        win_package = entry.get("win_package")
        if win_package is not None:
            if not isinstance(win_package, str):
                raise ValueError("'win_package' must be a string when provided")
            win_package = win_package.strip() or None
        min_minor_version = entry.get("min_minor_version")
        if min_minor_version is not None:
            if not isinstance(min_minor_version, str):
                raise ValueError("'min_minor_version' must be a string like '3.14'")
            min_minor_version = parse_minor_version(min_minor_version)
        specs.append(
            PackageSpec(
                package=package_name,
                executables=executables,
                win_package=win_package,
                min_minor_version=min_minor_version,
            )
        )
    return specs


def extract_minor_version(version: str) -> str | None:
    match = MINOR_VERSION_PATTERN.match(version)
    if not match:
        return None
    return f"{match.group('major')}.{match.group('minor')}"


def parse_minor_version(value: str) -> Tuple[int, int]:
    match = MINOR_VERSION_PATTERN.match(value.strip())
    if not match:
        raise ValueError(f"Expected a 'major.minor' version, got: {value}")
    return int(match.group("major")), int(match.group("minor"))


def minor_str_to_tuple(value: str) -> Tuple[int, int]:
    major, minor = value.split(".")
    return int(major), int(minor)


def sort_minor_versions(versions: Iterable[str]) -> List[str]:
    def sort_key(value: str) -> Sequence[int]:
        major, minor = value.split(".")
        return [int(major), int(minor)]

    return sorted({version for version in versions}, key=sort_key)


async def query_package_availability(
    gateway: Gateway,
    channel: Channel,
    platforms: Sequence[Tuple[str, Platform]],
    package_name: str,
) -> Dict[str, List[str]]:
    availability: Dict[str, Set[str]] = {}
    for platform_name, platform in platforms:
        try:
            results_per_channel = await gateway.query(
                channels=[channel],
                platforms=[platform],
                specs=[package_name],
                recursive=False,
            )
        except Exception as exc:  # pragma: no cover - rattler raises Rust errors
            raise RuntimeError(f"Failed to query {package_name} for {platform}: {exc}") from exc

        if not results_per_channel:
            continue

        records = results_per_channel[0]
        for record in records:
            version = getattr(record, "version", None)
            if not version:
                continue
            minor = extract_minor_version(str(version))
            if minor:
                availability.setdefault(minor, set()).add(platform_name)

    ordered = sort_minor_versions(availability.keys())
    return {minor: sorted(availability[minor]) for minor in ordered}


async def resolve_availability(
    channel: Channel,
    subdirs: Sequence[str],
    package_names: Sequence[str],
    gateway: Gateway,
) -> Dict[str, Dict[str, List[str]]]:
    platforms = [(subdir, Platform(subdir)) for subdir in subdirs]

    results: Dict[str, Dict[str, List[str]]] = {}
    for package in dict.fromkeys(package_names):
        results[package] = await query_package_availability(gateway, channel, platforms, package)
    return results


def filter_availability(
    availability: Dict[str, List[str]],
    allowed_platforms: Set[str],
) -> Dict[str, List[str]]:
    filtered: Dict[str, List[str]] = {}
    for minor, platforms in availability.items():
        subset = [platform for platform in platforms if platform in allowed_platforms]
        if subset:
            filtered[minor] = subset
    return filtered


def filter_by_min_version(
    availability: Dict[str, List[str]],
    min_version: Tuple[int, int] | None,
) -> Dict[str, List[str]]:
    if not min_version:
        return dict(availability)
    filtered: Dict[str, List[str]] = {}
    for minor_version, platforms in availability.items():
        if minor_str_to_tuple(minor_version) >= min_version:
            filtered[minor_version] = platforms
    return filtered


def dependency_constraint(package_name: str, minor_version: str) -> str:
    major, minor = minor_version.split(".")
    next_minor = f"{major}.{int(minor) + 1}"
    return f"{package_name}>={minor_version},<{next_minor}"


async def solve_for_platforms_async(
    channel: Channel,
    constraint: str,
    platform_names: Sequence[str],
    gateway: Gateway,
) -> List[str]:
    supported: List[str] = []
    for platform_name in platform_names:
        try:
            await solve_environment(
                channels=[channel],
                specs=[constraint],
                gateway=gateway,
                platforms=[Platform(platform_name), Platform("noarch")],
            )
        except SolverError:
            continue
        else:
            supported.append(platform_name)
    return supported


def write_env_file(
    output_root: Path,
    env_name: str,
    package_name: str,
    minor_version: str,
    platforms: Sequence[str],
    *,
    display_name: str | None = None,
    directory_name: str | None = None,
) -> Path:
    output_dir = output_root / (directory_name or package_name)
    output_dir.mkdir(parents=True, exist_ok=True)
    env_display = display_name or env_name
    platform_block = (
        "platforms:\n" + "\n".join(f"  - {platform}" for platform in platforms)
        if platforms
        else "platforms: []"
    )
    indented_platform_block = textwrap.indent(platform_block, "        ")
    contents = textwrap.dedent(
        f"""\
        name: {env_display}
        channels:
          - conda-forge
        dependencies:
          - {dependency_constraint(package_name, minor_version)}
{indented_platform_block}
        """
    )
    output_path = output_dir / f"{env_name}.yml"
    output_path.write_text(contents)
    return output_path


def sanitize_env_name(binary_name: str, minor_version: str, qualifier: str | None = None) -> str:
    safe_binary = binary_name.lower().replace("-", "_")
    env_name = f"{safe_binary}_{minor_version}"
    if qualifier:
        env_name = f"{env_name}_{qualifier}"
    return env_name


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    default_config = Path(__file__).with_name("config.json")
    parser.add_argument("--config", type=Path, default=default_config, help="Path to the mapping JSON file")
    return parser.parse_args(argv)


def find_repo_root(source_path: Path) -> Path:
    for candidate in [source_path.parent, *source_path.parents]:
        if (candidate / "MODULE.bazel").exists() or (candidate / ".git").exists():
            return candidate
    return source_path.parent


def write_generated_module(repo_root: Path, environment_packages: Dict[str, str]) -> None:
    module_path = repo_root / "generated.MODULE.bazel"
    env_items = sorted(environment_packages.items())
    env_lines = "\n".join(
        f'"{name}": "private/locks/{package}.lock",'
        for name, package in env_items
    )
    env_block = textwrap.indent(env_lines + "\n", "    ") if env_lines else ""
    contents = textwrap.dedent(
        f"""\
# This file is generated by generate/generate.py; do not edit.

environments = {{
{env_block}}}

conda = use_extension(
    "@rules_conda//conda:extensions.bzl",
    "conda",
)

[
    conda.environment(
        name = environment_name,
        lockfile = lockfile,
    )
    for environment_name, lockfile in environments.items()
]

[
    use_repo(conda, environment_name)
    for environment_name in environments
]

"""
    )
    module_path.write_text(contents)


def write_lock_definitions(
    repo_root: Path,
    package_files: Dict[str, List[Path]],
) -> None:
    private_dir = repo_root / "private"
    locks_dir = private_dir / "locks"
    locks_dir.mkdir(parents=True, exist_ok=True)
    lock_bzl = private_dir / "generated_locks.bzl"

    package_entries: List[str] = []
    for package in sorted(package_files):
        spec_lines = "\n".join(
            f'        "{path_item.relative_to(private_dir).as_posix()}",'
            for path_item in sorted(package_files[package])
        )
        block_body = f"\n{spec_lines}\n    " if spec_lines else ""
        package_entries.append(f'"{package}": [{block_body}],')
    package_block = (
        textwrap.indent("\n".join(package_entries) + "\n", "    ")
        if package_entries
        else ""
    )
    contents = textwrap.dedent(
        f"""\
# This file is generated by generate/generate.py; do not edit.

load("@rules_conda//conda/environment:defs.bzl", "lock_environments")
load("@rules_multirun//:defs.bzl", "multirun")

_PACKAGE_SPECS = {{
{package_block}}}

def define_conda_locks(*, macos_version, glibc_version):
    update_targets = []
    lock_paths = []
    for package, specs in _PACKAGE_SPECS.items():
        target = package + "_lock"
        lock_paths.append("private/locks/" + package + ".lock")
        lock_environments(
            name = target,
            environments = specs,
            lockfile = "locks/" + package + ".lock",
            macos_version = macos_version,
            glibc_version = glibc_version,
        )
        update_targets.append(":" + target + ".update")
    multirun(
        name = "lock_all",
        commands = update_targets,
    )
    ensure_lockfiles(
        name = "init_lock_files",
        lockfiles = lock_paths,
    )

def _ensure_lockfiles_impl(ctx):
    script = ctx.actions.declare_file(ctx.label.name + "_runner.sh")
    script_lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        'WORKSPACE_DIR="${{BUILD_WORKSPACE_DIRECTORY:-$PWD}}"',
    ]
    for lockfile in ctx.attr.lockfiles:
        script_lines.append("touch \\"${{WORKSPACE_DIR}}/%s\\"" % (lockfile))
    ctx.actions.write(script, "\\n".join(script_lines), is_executable = True)
    return [DefaultInfo(executable = script)]

ensure_lockfiles = rule(
    implementation = _ensure_lockfiles_impl,
    attrs = {{"lockfiles": attr.string_list()}},
    executable = True,
)

"""
    )

    lock_bzl.write_text(contents)


def write_tools_build_files(
    repo_root: Path,
    tool_versions: Dict[str, Dict[str, str]],
) -> None:
    tool_root = repo_root / "tool"
    if tool_root.exists():
        shutil.rmtree(tool_root)
    for tool_name, version_map in sorted(tool_versions.items()):
        package_dir = tool_root / tool_name
        package_dir.mkdir(parents=True, exist_ok=True)
        lines = [
            'load("@rules_conda//conda/environment:defs.bzl", "run_binary")',
            "",
        ]
        versions = sorted(
            version_map.items(),
            key=lambda item: minor_str_to_tuple(item[0]),
        )
        for minor_version, environment_name in versions:
            lines.append(
                textwrap.dedent(
                    f"""\
                    run_binary(
                        name = "{minor_version}",
                        environment = "@{environment_name}",
                        path = select({{
                            "@platforms//os:windows": "Library/bin/{tool_name}",
                            "//conditions:default": "bin/{tool_name}",
                        }}),
                        visibility = ["//visibility:public"],
                    )

                    """
                )
            )
        if versions:
            latest_version = versions[-1][0]
            lines.append(
                textwrap.dedent(
                    f"""\
                    alias(
                        name = "{tool_name}",
                        actual = ":{latest_version}",
                        visibility = ["//visibility:public"],
                    )

                    """
                )
            )
        build_path = package_dir / "BUILD.bazel"
        build_path.write_text("\n".join(lines).rstrip() + "\n")


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    package_specs = read_mapping(args.config)
    channel = Channel(DEFAULT_CHANNEL_URL)
    gateway = Gateway()
    source_path = Path(__file__).resolve()
    repo_root = find_repo_root(source_path)
    output_dir = repo_root / "private" / "specs"

    if output_dir.exists():
        shutil.rmtree(output_dir)

    packages_to_query: List[str] = [spec.package for spec in package_specs]
    packages_to_query.extend(
        spec.win_package for spec in package_specs if spec.win_package
    )

    availability_by_package = asyncio.run(
        resolve_availability(channel, DEFAULT_SUBDIRS, packages_to_query, gateway)
    )

    all_platforms = set(DEFAULT_SUBDIRS)
    windows_platforms = {name for name in all_platforms if Platform(name).is_windows}
    non_windows_platforms = all_platforms - windows_platforms
    windows_with_noarch = windows_platforms | ({"noarch"} if "noarch" in all_platforms else set())

    generated_files: Dict[str, List[Path]] = defaultdict(list)
    environment_packages: Dict[str, str] = {}
    tool_versions: Dict[str, Dict[str, str]] = defaultdict(dict)
    for spec in package_specs:
        base_availability = availability_by_package.get(spec.package, {})
        base_availability = filter_by_min_version(base_availability, spec.min_minor_version)
        allowed_base = non_windows_platforms if spec.win_package else all_platforms
        filtered_base: Dict[str, List[str]] = {}
        for minor_version, platforms in filter_availability(base_availability, allowed_base).items():
            filtered_base[minor_version] = [platform for platform in platforms if platform != "noarch"]

        if not filtered_base:
            message = f"warning: no versions found for {spec.package}"
            if spec.win_package:
                message += " on non-Windows platforms"
            print(message, file=sys.stderr)

        win_availability: Dict[str, List[str]] = {}
        if spec.win_package:
            raw_win = availability_by_package.get(spec.win_package, {})
            raw_win = filter_by_min_version(raw_win, spec.min_minor_version)
            win_availability = filter_availability(raw_win, windows_with_noarch)
            if not win_availability:
                print(
                    f"warning: no versions found for Windows package {spec.win_package}",
                    file=sys.stderr,
                )

        for minor_version, platforms in list(filtered_base.items()):
            if platforms:
                continue
            platform_candidates = sorted(
                p for p in allowed_base if p != "noarch"
            )
            if not platform_candidates:
                continue
            supported_platforms = asyncio.run(
                solve_for_platforms_async(
                    channel,
                    dependency_constraint(spec.package, minor_version),
                    platform_candidates,
                    gateway,
                )
            )
            if supported_platforms:
                filtered_base[minor_version] = supported_platforms
            else:
                filtered_base.pop(minor_version, None)

        for minor_version, platforms in filtered_base.items():
            env_name = sanitize_env_name(spec.package, minor_version)
            env_path = write_env_file(
                output_dir,
                env_name,
                spec.package,
                minor_version,
                platforms,
            )
            generated_files[spec.package].append(env_path)
            environment_packages.setdefault(env_name, spec.package)
            for tool in spec.executables:
                tool_versions[tool][minor_version] = env_name

        if spec.win_package:
            for minor_version, platforms in win_availability.items():
                base_env_name = sanitize_env_name(spec.package, minor_version)
                env_file_name = sanitize_env_name(spec.package, minor_version, qualifier="win")
                env_path = write_env_file(
                    output_dir,
                    env_file_name,
                    spec.win_package,
                    minor_version,
                    ["win-64"],
                    display_name=base_env_name,
                    directory_name=spec.package,
                )
                generated_files[spec.package].append(env_path)
                environment_packages.setdefault(base_env_name, spec.package)
                for tool in spec.executables:
                    tool_versions[tool][minor_version] = base_env_name

    for package, files in generated_files.items():
        print(f"{package}:")
        for file_path in files:
            print(f"  - {file_path}")

    write_lock_definitions(repo_root, generated_files)
    write_generated_module(repo_root, environment_packages)
    write_tools_build_files(repo_root, tool_versions)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
