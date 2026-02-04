// Seed initial characters
MERGE (founder:Character {uid: 'CHAR-001'})
SET founder.name = 'The Architect';

MERGE (subject:Character {uid: 'CHAR-002'})
SET subject.name = 'Subject 0';
