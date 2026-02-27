from __future__ import annotations

import sys
import time


class ProgressBar:
    """Terminal progress bar with optional ANSI color and ETA."""

    COLOR_RESET = "\033[0m"
    COLOR_CYAN = "\033[36m"
    COLOR_GREEN = "\033[32m"
    COLOR_YELLOW = "\033[33m"

    def __init__(self, total: int, *, label: str, width: int = 30, color: bool = True) -> None:
        self.total = max(0, total)
        self.label = label
        self.width = width
        self.color = color
        self.start = time.monotonic()
        self._last_render_len = 0

    @staticmethod
    def _format_seconds(seconds: float) -> str:
        seconds = max(0, int(seconds))
        minutes, sec = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        if hours:
            return f"{hours:d}:{minutes:02d}:{sec:02d}"
        return f"{minutes:02d}:{sec:02d}"

    def render(self, completed: int, failed: int = 0) -> str:
        ratio = completed / self.total if self.total else 1.0
        filled = min(self.width, int(ratio * self.width))
        bar = "#" * filled + "-" * (self.width - filled)

        elapsed = time.monotonic() - self.start
        if completed and completed < self.total:
            eta_seconds = (elapsed / completed) * (self.total - completed)
            eta = self._format_seconds(eta_seconds)
        elif completed >= self.total:
            eta = "00:00"
        else:
            eta = "--:--"

        label = self.label
        if self.color:
            label = f"{self.COLOR_CYAN}{label}{self.COLOR_RESET}"
            bar = f"{self.COLOR_GREEN}{bar}{self.COLOR_RESET}"

        status = f"{completed}/{self.total}"
        if failed:
            status_color = self.COLOR_YELLOW if self.color else ""
            reset = self.COLOR_RESET if self.color else ""
            status += f" | {status_color}failed: {failed}{reset}"

        return f"\r{label} [{bar}] {completed}/{self.total} ({ratio * 100:5.1f}%) ETA {eta} [{status}]"

    def print(self, completed: int, failed: int = 0) -> None:
        msg = self.render(completed, failed=failed)
        self._last_render_len = max(self._last_render_len, len(msg))
        print(msg, end="", file=sys.stderr, flush=True)

    def done(self, completed: int, failed: int = 0) -> None:
        self.print(completed, failed=failed)
        print(file=sys.stderr, flush=True)

