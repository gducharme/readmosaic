#!/usr/bin/env python3
"""Interactive launcher for scripts in a directory."""
from __future__ import annotations

import argparse
import curses
import json
import re
import shlex
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import List

from rich.console import Console
from rich.table import Table


LONG_OPTION_RE = re.compile(
    r"^\s*(?:-[A-Za-z],\s*)?(--[A-Za-z0-9][A-Za-z0-9\-_]*)(?:[ =]([^\s\]]+))?"
)
POSITIONAL_RE = re.compile(r"^\s{2,}([A-Za-z][A-Za-z0-9_]*)\s{2,}.+")


@dataclass
class ArgumentSpec:
    flag: str
    takes_value: bool
    placeholder: str = ""
    value: str = ""
    enabled: bool = False
    positional: bool = False


@dataclass
class ScriptSpec:
    path: Path
    blurb: str = ""
    arguments: List[ArgumentSpec] = field(default_factory=list)


def script_to_cache_payload(script: ScriptSpec) -> dict:
    return {
        "path": str(script.path),
        "blurb": script.blurb,
        "arguments": [
            {
                "flag": arg.flag,
                "takes_value": arg.takes_value,
                "placeholder": arg.placeholder,
                "value": arg.value,
                "enabled": arg.enabled,
                "positional": arg.positional,
            }
            for arg in script.arguments
        ],
    }


def script_from_cache_payload(payload: dict) -> ScriptSpec:
    return ScriptSpec(
        path=Path(payload["path"]),
        blurb=str(payload.get("blurb", "")),
        arguments=[
            ArgumentSpec(
                flag=str(arg.get("flag", "")),
                takes_value=bool(arg.get("takes_value", False)),
                placeholder=str(arg.get("placeholder", "")),
                value=str(arg.get("value", "")),
                enabled=bool(arg.get("enabled", False)),
                positional=bool(arg.get("positional", False)),
            )
            for arg in payload.get("arguments", [])
            if isinstance(arg, dict)
        ],
    )


class ScriptMenuApp:
    def __init__(self, scripts: List[ScriptSpec]) -> None:
        self.scripts = scripts
        self.script_index = 0
        self.field_index = 0
        self.console = Console()

    def run(self) -> None:
        curses.wrapper(self._main)

    def _main(self, stdscr: curses.window) -> None:
        curses.curs_set(0)
        stdscr.keypad(True)
        while True:
            selected = self._script_selection_menu(stdscr)
            if selected is None:
                return
            self._argument_menu(stdscr, selected)

    def _script_selection_menu(self, stdscr: curses.window) -> ScriptSpec | None:
        while True:
            stdscr.clear()
            h, w = stdscr.getmaxyx()
            stdscr.addstr(1, 2, "Mosaic Script Runner", curses.A_BOLD)
            stdscr.addstr(2, 2, "↑/↓ move • PgUp/PgDn jump • Enter select • Esc exit")

            visible_rows = max(1, (h - 5) // 2)
            start_index = _scroll_window_start(
                current_index=self.script_index,
                total_items=len(self.scripts),
                visible_items=visible_rows,
            )
            visible_scripts = self.scripts[start_index : start_index + visible_rows]

            for row, script in enumerate(visible_scripts):
                idx = start_index + row
                y = 4 + (row * 2)
                if y >= h - 1:
                    break
                name = script.path.name
                marker = "➤ " if idx == self.script_index else "  "
                attr = curses.A_REVERSE if idx == self.script_index else curses.A_NORMAL
                stdscr.addstr(y, 2, f"{marker}{name}", attr)
                blurb = script.blurb or "No description available."
                stdscr.addstr(y + 1, 6, blurb[: max(0, w - 8)], curses.A_DIM)
            key = stdscr.getch()
            if key == curses.KEY_UP:
                self.script_index = (self.script_index - 1) % len(self.scripts)
            elif key == curses.KEY_DOWN:
                self.script_index = (self.script_index + 1) % len(self.scripts)
            elif key == curses.KEY_PPAGE:
                self.script_index = max(0, self.script_index - visible_rows)
            elif key == curses.KEY_NPAGE:
                self.script_index = min(len(self.scripts) - 1, self.script_index + visible_rows)
            elif key in (10, 13, curses.KEY_ENTER):
                self.field_index = 0
                return self.scripts[self.script_index]
            elif key == 27:
                return None

    def _argument_menu(self, stdscr: curses.window, script: ScriptSpec) -> None:
        while True:
            stdscr.clear()
            h, _ = stdscr.getmaxyx()
            stdscr.addstr(1, 2, f"Script: {script.path.name}", curses.A_BOLD)
            stdscr.addstr(2, 2, "↑/↓ move • Enter edit/toggle • r run • Esc back")

            lines = [f"{idx+1}. {self._render_argument(arg)}" for idx, arg in enumerate(script.arguments)]
            lines += ["Run script", "Back to script list"]

            for idx, line in enumerate(lines):
                y = 4 + idx
                if y >= h - 1:
                    break
                attr = curses.A_REVERSE if idx == self.field_index else curses.A_NORMAL
                marker = "➤ " if idx == self.field_index else "  "
                stdscr.addstr(y, 2, f"{marker}{line}", attr)

            key = stdscr.getch()
            if key == curses.KEY_UP:
                self.field_index = (self.field_index - 1) % len(lines)
            elif key == curses.KEY_DOWN:
                self.field_index = (self.field_index + 1) % len(lines)
            elif key in (ord("r"), ord("R")):
                self._run_script(stdscr, script)
            elif key in (10, 13, curses.KEY_ENTER):
                if self.field_index < len(script.arguments):
                    self._edit_argument(stdscr, script.arguments[self.field_index])
                elif self.field_index == len(script.arguments):
                    self._run_script(stdscr, script)
                else:
                    return
            elif key == 27:
                return

    def _render_argument(self, arg: ArgumentSpec) -> str:
        if arg.positional:
            value = arg.value if arg.value else "<empty>"
            return f"{arg.flag} (required): {value}"
        if arg.takes_value:
            value = arg.value if arg.value else "<empty>"
            return f"{arg.flag} ({arg.placeholder or 'VALUE'}): {value}"
        status = "ON" if arg.enabled else "OFF"
        return f"{arg.flag}: {status}"

    def _edit_argument(self, stdscr: curses.window, arg: ArgumentSpec) -> None:
        if not arg.takes_value:
            arg.enabled = not arg.enabled
            return

        curses.echo()
        stdscr.clear()
        stdscr.addstr(1, 2, f"Enter value for {arg.flag} ({arg.placeholder or 'VALUE'})")
        stdscr.addstr(2, 2, "Leave blank to clear. Press Enter to confirm.")
        stdscr.addstr(4, 2, "> ")
        stdscr.refresh()
        value = stdscr.getstr(4, 4, 240).decode("utf-8").strip()
        arg.value = value
        curses.noecho()

    def _run_script(self, stdscr: curses.window, script: ScriptSpec) -> None:
        cmd = build_command(script)
        curses.endwin()
        self.console.print("\n[bold cyan]Running:[/bold cyan] " + shlex.join(cmd))
        result = subprocess.run(cmd, capture_output=True, text=True)
        self.console.print(f"[bold]{'Success' if result.returncode == 0 else 'Failed'}[/bold] (exit={result.returncode})")
        if result.stdout.strip():
            self.console.print("\n[green]STDOUT[/green]")
            self.console.print(result.stdout)
        if result.stderr.strip():
            self.console.print("\n[red]STDERR[/red]")
            self.console.print(result.stderr)
        input("\nPress Enter to return to the UI...")
        stdscr.refresh()


def build_command(script: ScriptSpec) -> List[str]:
    cmd = [sys.executable, str(script.path)]
    for arg in script.arguments:
        if arg.positional:
            if arg.value:
                cmd.append(arg.value)
            continue
        if arg.takes_value:
            if arg.value:
                cmd.extend([arg.flag, arg.value])
        elif arg.enabled:
            cmd.append(arg.flag)
    return cmd


def _scroll_window_start(current_index: int, total_items: int, visible_items: int) -> int:
    if total_items <= 0 or visible_items <= 0:
        return 0
    if total_items <= visible_items:
        return 0

    max_start = total_items - visible_items
    centered = current_index - (visible_items // 2)
    return max(0, min(centered, max_start))


def parse_help_arguments(script_path: Path) -> List[ArgumentSpec]:
    try:
        result = subprocess.run(
            [sys.executable, str(script_path), "--help"],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except subprocess.TimeoutExpired:
        return []

    help_text = result.stdout + "\n" + result.stderr
    args: List[ArgumentSpec] = []
    seen: set[str] = set()
    in_positional_block = False

    for line in help_text.splitlines():
        section_line = line.strip().lower()
        if section_line == "positional arguments:":
            in_positional_block = True
            continue
        if section_line in {"options:", "optional arguments:"}:
            in_positional_block = False

        if in_positional_block:
            positional_match = POSITIONAL_RE.match(line)
            if positional_match:
                name = positional_match.group(1)
                if name not in seen:
                    seen.add(name)
                    args.append(
                        ArgumentSpec(
                            flag=name,
                            takes_value=True,
                            placeholder=name.upper(),
                            positional=True,
                        )
                    )
            continue

        match = LONG_OPTION_RE.match(line)
        if not match:
            continue
        flag = match.group(1)
        placeholder = (match.group(2) or "").strip("[]")
        if flag in ("--help",) or flag in seen:
            continue
        seen.add(flag)
        takes_value = bool(placeholder)
        args.append(ArgumentSpec(flag=flag, takes_value=takes_value, placeholder=placeholder))

    return args


def parse_help_blurb(script_path: Path) -> str:
    try:
        result = subprocess.run(
            [sys.executable, str(script_path), "--help"],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except subprocess.TimeoutExpired:
        return ""

    help_text = result.stdout + "\n" + result.stderr
    lines = [line.strip() for line in help_text.splitlines()]

    for line in lines:
        lowered = line.lower()
        if not line or lowered.startswith("usage:"):
            continue
        return line
    return ""


def discover_scripts(scripts_dir: Path) -> List[ScriptSpec]:
    scripts: List[ScriptSpec] = []
    for path in sorted(scripts_dir.glob("*.py")):
        if path.name.startswith("__"):
            continue
        scripts.append(
            ScriptSpec(
                path=path,
                blurb=parse_help_blurb(path),
                arguments=parse_help_arguments(path),
            )
        )
    return scripts


def load_scripts_from_cache(cache_path: Path) -> List[ScriptSpec] | None:
    if not cache_path.exists():
        return None
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None

    if not isinstance(payload, dict):
        return None

    scripts_payload = payload.get("scripts")
    if not isinstance(scripts_payload, list):
        return None

    scripts: List[ScriptSpec] = []
    for item in scripts_payload:
        if not isinstance(item, dict):
            continue
        try:
            scripts.append(script_from_cache_payload(item))
        except KeyError:
            continue
    return scripts


def write_scripts_cache(cache_path: Path, scripts: List[ScriptSpec]) -> None:
    payload = {
        "cache_version": 1,
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "scripts": [script_to_cache_payload(script) for script in scripts],
    }
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def list_scripts(console: Console, scripts: List[ScriptSpec]) -> None:
    table = Table(title="Discovered Scripts")
    table.add_column("Script")
    table.add_column("Description")
    table.add_column("Arguments", justify="right")
    for script in scripts:
        table.add_row(script.path.name, script.blurb or "-", str(len(script.arguments)))
    console.print(table)


def parse_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Interactive menu for Mosaic scripts.")
    parser.add_argument("--scripts-dir", default="scripts", help="Directory containing scripts.")
    parser.add_argument(
        "--cache-file",
        help="Path to script discovery cache JSON (defaults to <scripts-dir>/.script_menu_cache.json).",
    )
    parser.add_argument(
        "--regen",
        action="store_true",
        help="Delete any existing cache and regenerate script metadata from scratch.",
    )
    parser.add_argument("--list", action="store_true", help="List discovered scripts and exit.")
    return parser.parse_args()


def main() -> None:
    args = parse_cli_args()
    scripts_dir = Path(args.scripts_dir)
    console = Console()

    if not scripts_dir.exists():
        console.print(f"[red]Scripts directory not found:[/red] {scripts_dir}")
        raise SystemExit(1)

    cache_path = Path(args.cache_file) if args.cache_file else scripts_dir / ".script_menu_cache.json"

    if args.regen and cache_path.exists():
        cache_path.unlink()

    scripts = load_scripts_from_cache(cache_path)
    if scripts is None:
        scripts = discover_scripts(scripts_dir)
        write_scripts_cache(cache_path, scripts)
    if not scripts:
        console.print("[yellow]No scripts discovered.[/yellow]")
        return

    if args.list:
        list_scripts(console, scripts)
        return

    app = ScriptMenuApp(scripts)
    app.run()


if __name__ == "__main__":
    main()
