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
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Set, Tuple

from rattler.channel import Channel
from rattler.exceptions import SolverError
from rattler.platform import Platform
from rattler.repo_data.gateway import Gateway
from rattler.solver.solver import solve as solve_environment


DEFAULT_CHANNEL_URL = "https://conda.anaconda.org/conda-forge"
HELP_TEST_SCRIPT = "//private:tool_smoke_test.sh"
PLATFORM_CONFIG_SETTINGS = {
    "linux-64": "//:platform_linux_64",
    "linux-aarch64": "//:platform_linux_aarch64",
    "linux-armv7l": "//:platform_linux_armv7l",
    "linux-ppc64le": "//:platform_linux_ppc64le",
    "linux-s390x": "//:platform_linux_s390x",
    "osx-64": "//:platform_macos_x86_64",
    "osx-arm64": "//:platform_macos_arm64",
    "win-64": "//:platform_windows_x86_64",
    "win-arm64": "//:platform_windows_arm64",
}
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
    executable_overrides: Dict[str, Dict[str, str]] = field(default_factory=dict)


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
        executable_overrides_raw = entry.get("executable_overrides", {})
        executable_overrides: Dict[str, Dict[str, str]] = {}
        if executable_overrides_raw:
            if not isinstance(executable_overrides_raw, dict):
                raise ValueError("'executable_overrides' must be a dictionary")
            for exe_name, platform_map in executable_overrides_raw.items():
                if exe_name not in executables:
                    raise ValueError(f"Override provided for unknown executable '{exe_name}'")
                if not isinstance(platform_map, dict):
                    raise ValueError(f"Platform overrides for '{exe_name}' must be a dictionary")
                normalized_map: Dict[str, str] = {}
                for platform_label, override_name in platform_map.items():
                    if not isinstance(platform_label, str) or not isinstance(override_name, str):
                        raise ValueError("Platform override mapping must use string keys and values")
                    platform_label = platform_label.strip()
                    override_name = override_name.strip()
                    if not platform_label or not override_name:
                        raise ValueError("Platform override entries must be non-empty")
                    normalized_map[platform_label] = override_name
                if normalized_map:
                    executable_overrides[exe_name] = normalized_map
        specs.append(
            PackageSpec(
                package=package_name,
                executables=executables,
                win_package=win_package,
                min_minor_version=min_minor_version,
                executable_overrides=executable_overrides,
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
            if not minor:
                continue

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


MODULE_TEMPLATE_PLACEHOLDER = "{{GENERATED_ENVIRONMENTS}}"


def render_environment_block(environment_packages: Dict[str, str]) -> str:
    env_items = sorted(environment_packages.items())
    if not env_items:
        return ""
    return "\n".join(
        f'    "{name}": "private/locks/{package}.lock",'
        for name, package in env_items
    )


def write_module_file(repo_root: Path, environment_packages: Dict[str, str]) -> None:
    template_path = Path(__file__).with_name("MODULE.bazel.tmpl")
    template = template_path.read_text()
    environment_block = render_environment_block(environment_packages)
    module_contents = template.replace(MODULE_TEMPLATE_PLACEHOLDER, environment_block)
    module_path = repo_root / "MODULE.bazel"
    module_path.write_text(module_contents)
    generated_module_path = repo_root / "generated.MODULE.bazel"
    if generated_module_path.exists():
        generated_module_path.unlink()


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
    package_block = textwrap.indent("\n".join(package_entries), "    ")
    template_path = Path(__file__).with_name("locks.bzl.tmpl")
    template = template_path.read_text()
    contents = template.replace("{{PACKAGE_SPECS}}", package_block)
    lock_bzl.write_text(contents)


def render_target_compatible_with(platforms: Sequence[str], *, context: str) -> str:
    labels = []
    for platform in sorted(set(platforms)):
        label = PLATFORM_CONFIG_SETTINGS.get(platform)
        if label:
            labels.append(label)
        else:
            print(
                f"warning: no config_setting mapping for platform '{platform}' used in {context}",
                file=sys.stderr,
            )
    if not labels:
        return "[]"
    entries = "\n".join(f'        "{label}": [],' for label in labels)
    return (
        "select({\n"
        f"{entries}\n"
        '        "//conditions:default": ["@platforms//:incompatible"],\n'
        "    })"
    )


def write_tools_build_files(
    repo_root: Path,
    tool_versions: Dict[str, Dict[str, str]],
    environment_platforms: Dict[str, Set[str]],
    tool_executable_overrides: Dict[str, Dict[str, str]],
) -> None:
    tool_root = repo_root / "tool"
    if tool_root.exists():
        shutil.rmtree(tool_root)
    for tool_name, version_map in sorted(tool_versions.items()):
        package_dir = tool_root / tool_name
        package_dir.mkdir(parents=True, exist_ok=True)
        lines = [
            'load("@rules_conda//conda/environment:defs.bzl", "run_binary")',
            'load("@rules_shell//shell:sh_test.bzl", "sh_test")',
            "",
        ]
        versions = sorted(
            version_map.items(),
            key=lambda item: minor_str_to_tuple(item[0]),
        )
        for minor_version, environment_name in versions:
            windows_binary = (
                tool_name
                if tool_name.endswith(".exe")
                else f"{tool_name}.exe"
            )
            overrides = tool_executable_overrides.get(tool_name, {})
            select_entries = dict(overrides)
            select_entries.setdefault("@platforms//os:windows", windows_binary)
            select_entries.setdefault("//conditions:default", tool_name)
            order = []
            if "@platforms//os:windows" in select_entries:
                order.append("@platforms//os:windows")
            other_labels = [label for label in select_entries if label not in ("@platforms//os:windows", "//conditions:default")]
            order.extend(sorted(other_labels))
            if "//conditions:default" in select_entries:
                order.append("//conditions:default")
            select_body = "\n".join(
                f'                            "{label}": "{select_entries[label]}",'
                for label in order
            )
            lines.append(
                textwrap.dedent(
                    f"""\
                    run_binary(
                        name = "{minor_version}",
                        environment = "@{environment_name}",
                        executable = select({{
{select_body}
                        }}),
                        visibility = ["//visibility:public"],
                    )

                    """
                )
            )
        if versions:
            latest_version = versions[-1][0]
            latest_environment = versions[-1][1]
            compatibility_expr = render_target_compatible_with(
                sorted(environment_platforms.get(latest_environment, [])),
                context=f"{tool_name}:{latest_version}",
            )
            indented_compat = textwrap.indent(compatibility_expr, " " * 12)
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
            compatibility_block = textwrap.indent(compatibility_expr, " " * 4)
            smoke_name = f"{tool_name}_smoke_test"
            lines.append(
                "\n".join(
                    [
                        "sh_test(",
                        f'    name = "{smoke_name}",',
                        f'    srcs = ["{HELP_TEST_SCRIPT}"],',
                        f'    args = ["$(location :{latest_version})"],',
                        f'    data = [":{latest_version}"],',
                        "    target_compatible_with =",
                        f"{compatibility_block},",
                        ")",
                        "",
                    ]
                )
            )
        build_path = package_dir / "BUILD.bazel"
        build_path.write_text("\n".join(lines).rstrip() + "\n")


def write_readmes(
    repo_root: Path,
    tool_versions: Dict[str, Dict[str, str]],
    environment_platforms: Dict[str, Set[str]],
) -> None:
    tool_root = repo_root / "tool"
    tool_readme = tool_root / "README.md"
    tool_lines = [
        "<!-- This file is generated by generate/generate.py; do not edit. -->",
        "",
        "# Bazel Tool Catalog",
        "",
        "Each subdirectory exposes Bazel targets for a specific tool. Select a tool below to view the supported versions and platforms.",
        "",
    ]
    for tool_name in sorted(tool_versions):
        tool_lines.append(f"- [{tool_name}](./{tool_name}/README.md)")
    tool_lines.append("")
    tool_readme.write_text("\n".join(tool_lines))

    for tool_name, version_map in sorted(tool_versions.items()):
        per_tool_readme = tool_root / tool_name / "README.md"
        per_tool_lines = [
            "<!-- This file is generated by generate/generate.py; do not edit. -->",
            "",
            f"# {tool_name}",
            "",
            "## Supported Versions",
            "",
            "| Version | Platforms |",
            "| --- | --- |",
        ]
        for minor_version, environment_name in sorted(
            version_map.items(),
            key=lambda item: minor_str_to_tuple(item[0]),
        ):
            platforms = sorted(environment_platforms.get(environment_name, []))
            platform_text = ", ".join(platforms) if platforms else "n/a"
            per_tool_lines.append(f"| {minor_version} | {platform_text} |")
        per_tool_lines.append("")
        per_tool_readme.write_text("\n".join(per_tool_lines))


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
    tool_executable_overrides: Dict[str, Dict[str, str]] = defaultdict(dict)
    environment_platforms: Dict[str, Set[str]] = defaultdict(set)
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
            environment_platforms[env_name].update(platforms)
            for tool in spec.executables:
                tool_versions[tool][minor_version] = env_name
                overrides = spec.executable_overrides.get(tool)
                if overrides:
                    tool_executable_overrides[tool].update(overrides)

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
                environment_platforms[base_env_name].update(["win-64"])
                for tool in spec.executables:
                    tool_versions[tool][minor_version] = base_env_name
                    overrides = spec.executable_overrides.get(tool)
                    if overrides:
                        tool_executable_overrides[tool].update(overrides)

    for package, files in generated_files.items():
        print(f"{package}:")
        for file_path in files:
            print(f"  - {file_path}")

    write_lock_definitions(repo_root, generated_files)
    write_module_file(repo_root, environment_packages)
    write_tools_build_files(repo_root, tool_versions, environment_platforms, tool_executable_overrides)
    write_readmes(repo_root, tool_versions, environment_platforms)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
