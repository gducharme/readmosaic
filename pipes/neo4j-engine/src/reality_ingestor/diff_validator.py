from __future__ import annotations

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from .models import DiffReport, ResolutionPlan


class DiffValidator:
    def __init__(self, config):
        self.console = Console()
        self.config = config

    def summarize(self, plan: ResolutionPlan, extracted: dict[str, object]) -> DiffReport:
        green = []
        yellow = []
        red = []

        for entity in plan.new_entities:
            green.append({"summary": "New entity", "details": f"{entity['name']} ({entity['type']})"})
        for event in extracted.get("events", []):
            green.append({"summary": "New event", "details": event.get("description", "")})
        for state in extracted.get("state_changes", []):
            green.append({"summary": "State change", "details": f"{state['entity_temp_id']} -> {state['attribute']}"})
        for rel in extracted.get("relationships", []):
            green.append({"summary": "Relationship", "details": rel.get("nature", "")})

        if plan.conflicts:
            red.append(
                {"summary": "Conflicts", "details": plan.conflicts[0].get("reason", "")}
            )

        decision = self._prompt(green, yellow, red)
        return DiffReport(run_id=self.config.run_id, green=green, yellow=yellow, red=red, decision=decision)

    def _prompt(self, green, yellow, red) -> dict[str, str]:
        table = Table(title="Reality Ingestor Diff", show_lines=True)
        table.add_column("Color")
        table.add_column("Summary")
        table.add_column("Details")
        for row in green:
            table.add_row("green", row["summary"], row["details"])
        for row in yellow:
            table.add_row("yellow", row["summary"], row["details"])
        for row in red:
            table.add_row("red", row["summary"], row["details"])
        self.console.print(table)
        decision_mode = self.config.diff_decision.lower()
        if decision_mode == "prompt":
            self.console.print(Panel("[A]ccept   [E]dit JSON   [R]eject", title="Decision"))
            choice = self.console.input("Choose action [A/E/R]: ").strip().lower()
        else:
            choice = decision_mode[0]
        if choice == "r":
            return {"status": "rejected", "note": "User rejected commit."}
        if choice == "e":
            return {"status": "edited", "note": "User will edit payload before retry."}
        return {"status": "accepted", "note": None}
