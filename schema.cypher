// Core schema constraints and indexes
CREATE CONSTRAINT character_uid IF NOT EXISTS
FOR (c:Character)
REQUIRE c.uid IS UNIQUE;

CREATE CONSTRAINT event_uid IF NOT EXISTS
FOR (e:Event)
REQUIRE e.uid IS UNIQUE;

CREATE CONSTRAINT location_name IF NOT EXISTS
FOR (l:Location)
REQUIRE l.name IS UNIQUE;

CREATE VECTOR INDEX character_bio_index IF NOT EXISTS
FOR (c:Character) ON (c.embedding)
OPTIONS {
  indexConfig: {
    `vector.dimensions`: 1536,
    `vector.similarity_function`: 'cosine'
  }
};
