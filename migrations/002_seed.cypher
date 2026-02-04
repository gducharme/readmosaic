// Seed initial character, event, and relationships
MERGE (founder:Character {uid: 'char_001'})
SET founder.name = 'The Founder';

MERGE (event:Event {uid: 'evt_001'})
SET event.name = 'Genesis Encounter',
    event.type = 'SEXUAL_IMPRINT';

MATCH (founder:Character {uid: 'char_001'})
MATCH (event:Event {uid: 'evt_001'})
MERGE (founder)-[:PARTICIPATED {role: 'Apex'}]->(event);

MERGE (year:Year {value: 2024})
WITH year
MATCH (event:Event {uid: 'evt_001'})
MERGE (event)-[:OCCURRED_IN]->(year);
