# TICKET-008 — Minimal tests and fixtures for the contract

## Problem
Without fixtures and lightweight tests, the contract and merger logic will drift as scripts evolve.

## Goal
Add minimal tests (or a validation harness) that ensure:
- unified bundle contract is stable
- merger produces expected keys and types
- regression fixtures exist for at least one small sample manuscript

## Suggested approach
- Add a tiny sample input manuscript (short markdown) in a test/fixtures location.
- Run the merger on pre-canned tool outputs (or stubbed outputs) to keep tests fast.
- Validate output JSON against the schema (if implemented) or strict key/type checks.

## Acceptance criteria
- A `pytest` (or equivalent) test run validates the unified bundle structure.
- Fixtures are small and checked into repo.

## Dependencies
- TICKET-001 (contract definition)
- TICKET-004 (merger exists)

