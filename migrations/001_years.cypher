// Build Year nodes and NEXT_YEAR relationships
UNWIND range(2020, 2050) AS year
MERGE (:Year {value: year});

UNWIND range(2020, 2049) AS year
MATCH (current:Year {value: year})
MATCH (next:Year {value: year + 1})
MERGE (current)-[:NEXT_YEAR]->(next);
