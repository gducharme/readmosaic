Q_FETCH_ENTITIES = """
MATCH (e:Entity)
RETURN e.uuid AS uuid, e.name AS name, labels(e) AS labels, e.aliases AS aliases, e.baseline_state AS baseline_state
LIMIT $limit
"""

Q_FETCH_ENTITY_STATES = """
UNWIND $entity_uuids AS entity_uuid
MATCH (e:Entity {uuid: entity_uuid})-[:HAS_STATE]->(s:State)
RETURN e.uuid AS entity_uuid,
       s.uuid AS state_uuid,
       s.attribute AS attribute,
       s.value AS value,
       s.valid_from_event AS valid_from_event,
       s.valid_until_event AS valid_until_event,
       s.created_at AS created_at
ORDER BY s.created_at DESC
LIMIT $limit
"""

Q_FETCH_RELATIONSHIPS = """
MATCH (a:Entity)-[r:INTERACTS_WITH]->(b:Entity)
RETURN a.uuid AS source_uuid,
       b.uuid AS target_uuid,
       r.nature AS nature,
       r.weight AS weight,
       r.context AS context
LIMIT $limit
"""

Q_FETCH_EVENT_TYPES = """
MATCH (e:Event)
RETURN e.type AS event_type, count(*) AS freq
ORDER BY freq DESC
LIMIT $limit
"""

Q_UPSERT_CHUNK = """
MERGE (c:Chunk {hash: $hash})
ON CREATE SET
  c.text = $text,
  c.sequence_id = $sequence_id,
  c.chapter_id = $chapter_id,
  c.source_path = $source_path
ON MATCH SET
  c.text = coalesce(c.text, $text),
  c.sequence_id = coalesce(c.sequence_id, $sequence_id),
  c.chapter_id = coalesce(c.chapter_id, $chapter_id),
  c.source_path = coalesce(c.source_path, $source_path)
RETURN c.hash AS hash
"""

Q_UPSERT_ENTITY_BASE = """
MERGE (e:Entity {uuid: $uuid})
ON CREATE SET
  e.name = $name,
  e.aliases = $aliases,
  e.aliases_text = $aliases_text,
  e.baseline_state = $baseline_state,
  e.embedding = $embedding
ON MATCH SET
  e.name = coalesce(e.name, $name),
  e.aliases = CASE WHEN size(coalesce(e.aliases, [])) = 0 THEN $aliases ELSE e.aliases END,
  e.aliases_text = CASE WHEN e.aliases_text IS NULL THEN $aliases_text ELSE e.aliases_text END,
  e.baseline_state = coalesce(e.baseline_state, $baseline_state),
  e.embedding = coalesce(e.embedding, $embedding)
RETURN e.uuid AS uuid
"""

Q_SET_ENTITY_SUBLABEL = """
MATCH (e:Entity {uuid: $uuid})
CALL apoc.create.addLabels(e, [$entity_type]) YIELD node
RETURN node.uuid AS uuid, labels(node) AS labels
"""

Q_UPSERT_EVENT = """
MERGE (ev:Event {uuid: $event_uuid})
ON CREATE SET
  ev.type = $event_type,
  ev.description = $description,
  ev.timestamp = $timestamp,
  ev.sequence = $sequence,
  ev.chapter_id = $chapter_id
ON MATCH SET
  ev.type = coalesce(ev.type, $event_type),
  ev.description = coalesce(ev.description, $description),
  ev.timestamp = coalesce(ev.timestamp, $timestamp),
  ev.sequence = coalesce(ev.sequence, $sequence),
  ev.chapter_id = coalesce(ev.chapter_id, $chapter_id)
RETURN ev.uuid AS uuid
"""

Q_LINK_EVENT_DOCUMENTED_BY_CHUNK = """
MATCH (ev:Event {uuid: $event_uuid})
MATCH (c:Chunk {hash: $chunk_hash})
MERGE (ev)-[:DOCUMENTED_BY]->(c)
"""

Q_LINK_EVENT_OCCURRED_IN_LOCATION = """
MATCH (ev:Event {uuid: $event_uuid})
MATCH (loc:Entity:Location {uuid: $location_uuid})
MERGE (ev)-[:OCCURRED_IN]->(loc)
"""

Q_LINK_ENTITY_PARTICIPATED_IN_EVENT = """
MATCH (ent:Entity {uuid: $entity_uuid})
MATCH (ev:Event {uuid: $event_uuid})
MERGE (ent)-[r:PARTICIPATED_IN]->(ev)
ON CREATE SET
  r.role = $role,
  r.intent = $intent
ON MATCH SET
  r.role = $role,
  r.intent = coalesce($intent, r.intent)
"""

Q_LINK_EVENT_CAUSED_EVENT = """
MATCH (cause:Event {uuid: $cause_event_uuid})
MATCH (effect:Event {uuid: $effect_event_uuid})
MERGE (cause)-[:CAUSED]->(effect)
"""

Q_LINK_EVENT_NEXT = """
MATCH (a:Event {uuid: $from_event_uuid})
MATCH (b:Event {uuid: $to_event_uuid})
MERGE (a)-[:NEXT]->(b)
"""

Q_CLOSE_OPEN_STATE_FOR_ATTRIBUTE = """
MATCH (e:Entity {uuid: $entity_uuid})-[:HAS_STATE]->(old:State)
WHERE old.attribute = $attribute
  AND old.valid_until_event IS NULL
SET old.valid_until_event = $valid_until_event,
    old.closed_at = $closed_at
RETURN count(old) AS closed_count
"""

Q_UPSERT_STATE = """
MERGE (s:State {uuid: $state_uuid})
ON CREATE SET
  s.attribute = $attribute,
  s.value = $value,
  s.valid_from_event = $valid_from_event,
  s.valid_until_event = NULL,
  s.created_at = $created_at
RETURN s.uuid AS uuid
"""

Q_LINK_ENTITY_HAS_STATE = """
MATCH (e:Entity {uuid: $entity_uuid})
MATCH (s:State {uuid: $state_uuid})
MERGE (e)-[:HAS_STATE]->(s)
"""

Q_UPSERT_INTERACTS_WITH = """
MATCH (a:Entity {uuid: $source_uuid})
MATCH (b:Entity {uuid: $target_uuid})
MERGE (a)-[r:INTERACTS_WITH {
  nature: $nature,
  context: $context,
  source_event_uuid: $source_event_uuid
}]->(b)
SET r.weight = $weight,
    r.updated_at = $updated_at
"""

Q_PROMOTE_ENTITY_NAME = """
MATCH (e:Entity {uuid: $uuid})
SET e.name = $new_name,
    e.aliases = CASE
      WHEN $old_name IS NULL OR trim($old_name) = '' THEN coalesce(e.aliases, [])
      WHEN $old_name IN coalesce(e.aliases, []) THEN coalesce(e.aliases, [])
      ELSE coalesce(e.aliases, []) + $old_name
    END,
    e.aliases_text = $aliases_text
RETURN e.uuid AS uuid
"""
