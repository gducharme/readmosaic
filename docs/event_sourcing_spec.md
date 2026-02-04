# Event Sourcing Schema Definition

## Purpose
This document defines the canonical schema rules and seed data for event sourcing within the Mosaic graph. It exists to ensure every event is grounded in time, space, and causality, with explicitly tracked participants.

## Core Constraints

### Location Uniqueness
* **Constraint:** `Location` nodes must have a unique `name`.
* **Rationale:** Events must be anchored to a stable physical space (e.g., "The Archive", "Sector 4 Apartment").

## Event Factory (`log_event`)

### Function Signature
`log_event(tx, uid, type, timestamp, description, location_name, actors, parent_event_uid=None)`

### Parameters
* `uid` (string): Unique identifier for the event.
* `type` (string): Domain-specific event type.
* `timestamp` (int or ISO-8601 string): Stored on the event, used to locate the associated year.
* `description` (string): Human-readable detail about the event.
* `location_name` (string): The physical location where the event occurred.
* `actors` (dict): `{character_uid: role_string}` describing who participated.
* `parent_event_uid` (string, optional): UID of the causal parent event.

### Transaction Semantics
Within a single transactional Cypher execution, the function MUST:

1. **Merge Time**: Locate or create the `Year` node for the event.
2. **Merge Location**: Locate or create the `Location` node.
3. **Create Event**: Create the `Event` node with `{uid, type, description, timestamp}`.
4. **Link Time & Space**:
   * `(Event)-[:OCCURRED_IN]->(Year)`
   * `(Event)-[:TOOK_PLACE_AT]->(Location)`
5. **Link Actors**:
   * Match each `Character` by UID.
   * Create `(Character)-[:PARTICIPATED {role}]->(Event)` relationships.
6. **Link Causality** (when `parent_event_uid` is provided):
   * Match the parent event.
   * Create `(Event)-[:CAUSED_BY]->(ParentEvent)`.

## Seed Data: The Mosaic Genesis
All initial events should be created via `log_event` to ensure the causal chain is constructed consistently.

### Characters
* **Founder**: `CHAR-001` / "The Architect"
* **Subject Zero**: `CHAR-002` / "Subject 0"

### Locations
* "Sector 4 Apartment" (mundane origin point)

### Events

#### EVT-001 — The Meeting
* **Type:** `SOCIAL_RITUAL`
* **Timestamp:** `2024`
* **Description:** "Initial contact initiated by Founder."
* **Location:** "Sector 4 Apartment"
* **Actors:**
  * `CHAR-001`: Initiator
  * `CHAR-002`: Target

#### EVT-002 — The Breach
* **Type:** `BIOLOGICAL_IMPRINT`
* **Timestamp:** `2024`
* **Description:** "Consent protocols bypassed via epigenetic trigger."
* **Location:** "Seaside"
* **Actors:**
  * `CHAR-001`: Apex
  * `CHAR-002`: Vessel
* **Parent Event:** `EVT-001`

## Data Integrity Notes
* Characters must exist before events that reference them.
* Each event MUST be attached to a `Year` and a `Location`.
* Causal relationships should always point from a newer event to the event that caused it.
