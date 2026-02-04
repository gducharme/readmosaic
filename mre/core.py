"""Core engine for applying Mosaic Revision Engine diagnostics."""
from __future__ import annotations

import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional


@dataclass
class StagedEdit:
    diagnostic_id: str
    paragraph_index: int
    action: str
    original_text: str
    modified_text: str
    metadata: Dict[str, Any] = field(default_factory=dict)


class MREEngine:
    """Apply diagnostic-guided edits to a manuscript."""

    def __init__(
        self,
        manuscript_text: str,
        diagnostics: Mapping[str, Any] | List[Mapping[str, Any]],
        tool_dir: Path | str,
        lm_client: Any,
    ) -> None:
        self.manuscript_text = manuscript_text
        self.diagnostics = diagnostics
        self.tool_dir = Path(tool_dir)
        self.lm_client = lm_client

        self._paragraphs: List[str] = self._split_paragraphs(manuscript_text)
        self._session_buffer: MutableMapping[int, str] = {
            idx: paragraph for idx, paragraph in enumerate(self._paragraphs)
        }
        self._staged_edits: List[StagedEdit] = []

    @property
    def session_buffer(self) -> Mapping[int, str]:
        return dict(self._session_buffer)

    @property
    def staged_edits(self) -> List[StagedEdit]:
        return list(self._staged_edits)

    def run(self) -> str:
        """Run the engine against all diagnostic items and return the updated manuscript."""
        for item in self._iter_diagnostics():
            self._apply_diagnostic(item)
        return self._join_paragraphs(self._session_buffer)

    def apply_tool_call(
        self,
        tool_call: Mapping[str, Any],
        paragraph_index: int,
        input_text: str,
    ) -> str:
        """Execute a tool call and return the updated text for a paragraph."""
        script = tool_call.get("script") or tool_call.get("tool")
        if not script:
            raise ValueError("Tool call missing 'script' field")
        args = tool_call.get("args", [])
        if not isinstance(args, list):
            raise ValueError("Tool call 'args' must be a list")
        script_path = self.tool_dir / script
        if not script_path.exists():
            raise FileNotFoundError(f"Tool script not found: {script_path}")
        process = subprocess.run(
            [sys.executable, str(script_path), *args],
            input=input_text.encode("utf-8"),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        if process.returncode != 0:
            stderr = process.stderr.decode("utf-8", errors="replace")
            raise RuntimeError(
                f"Tool call failed for paragraph {paragraph_index}: {stderr}"
            )
        return process.stdout.decode("utf-8", errors="replace").strip()

    def _apply_diagnostic(self, item: Mapping[str, Any]) -> None:
        paragraph_index = self._get_paragraph_index(item)
        action = self._get_action(item)
        original_text = self._session_buffer.get(paragraph_index, "")
        if not original_text:
            return

        if action == "forge":
            modified_text = self._handle_forge(item, paragraph_index, original_text)
        elif action == "call":
            modified_text = self._handle_call(item, paragraph_index, original_text)
        else:
            return

        if modified_text and modified_text != original_text:
            self._session_buffer[paragraph_index] = modified_text
            self._staged_edits.append(
                StagedEdit(
                    diagnostic_id=str(item.get("id", f"para-{paragraph_index}")),
                    paragraph_index=paragraph_index,
                    action=action,
                    original_text=original_text,
                    modified_text=modified_text,
                    metadata={
                        "diagnostic": dict(item),
                    },
                )
            )

    def _handle_forge(
        self,
        item: Mapping[str, Any],
        paragraph_index: int,
        paragraph_text: str,
    ) -> str:
        prompt = item.get("prompt") or item.get("instruction") or "Revise this paragraph."
        payload = {
            "paragraph_index": paragraph_index,
            "instruction": prompt,
            "text": paragraph_text,
        }
        response = self._call_lm(payload)
        return response.strip() if response else paragraph_text

    def _handle_call(
        self,
        item: Mapping[str, Any],
        paragraph_index: int,
        paragraph_text: str,
    ) -> str:
        tool_call = item.get("tool_call") or item.get("call") or {}
        if not tool_call:
            return paragraph_text
        return self.apply_tool_call(tool_call, paragraph_index, paragraph_text)

    def _call_lm(self, payload: Mapping[str, Any]) -> str:
        if hasattr(self.lm_client, "complete"):
            return self.lm_client.complete(payload)
        if hasattr(self.lm_client, "chat"):
            return self.lm_client.chat(payload)
        if callable(self.lm_client):
            return self.lm_client(payload)
        raise AttributeError("LM client does not support complete/chat/callable interface")

    def _iter_diagnostics(self) -> Iterable[Mapping[str, Any]]:
        if isinstance(self.diagnostics, list):
            return list(self.diagnostics)
        items = self.diagnostics.get("items") or self.diagnostics.get("diagnostics")
        if items is None:
            return []
        if not isinstance(items, list):
            raise ValueError("Diagnostics items must be a list")
        return items

    @staticmethod
    def _split_paragraphs(text: str) -> List[str]:
        paragraphs = [para.strip() for para in text.split("\n\n")]
        return [para for para in paragraphs if para]

    @staticmethod
    def _join_paragraphs(buffer: Mapping[int, str]) -> str:
        ordered = [text for _, text in sorted(buffer.items(), key=lambda item: item[0])]
        return "\n\n".join(ordered)

    @staticmethod
    def _get_paragraph_index(item: Mapping[str, Any]) -> int:
        for key in ("paragraph_index", "paragraph", "para_index", "index"):
            if key in item:
                return int(item[key])
        raise KeyError("Diagnostic item missing paragraph index")

    @staticmethod
    def _get_action(item: Mapping[str, Any]) -> str:
        action = item.get("action") or item.get("branch") or item.get("type")
        if not action:
            return ""
        return str(action).strip().lower()
