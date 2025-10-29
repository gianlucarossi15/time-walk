MATCH(n) detach delete n;

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///synthea/patients_10000.csv" AS row RETURN row',
  'CREATE (p:Patient {
        patientId: row.Id,
        birthdate: date(row.BIRTHDATE),
        deathdate: date(row.DEATHDATE),
        ssn: toString(row.SSN),
        prefix: toString(row.PREFIX),
        first: toString(row.FIRST),
        last: toString(row.LAST),
        race: toString(row.RACE),
        ethnicity: toString(row.ETHNICITY),
        gender: toString(row.GENDER),
        address: toString(row.ADDRESS),
        city: toString(row.CITY),
        state: toString(row.STATE),
        county: toString(row.COUNTY),
        zip: toInteger(row.ZIP),
        healthcareCoverage: toFloat(row.HEALTHCARE_COVERAGE),
        income: toFloat(row.INCOME)
  })',
  {batchSize:1000}
);

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///synthea/timeseries_10000.csv" AS row RETURN row',
  '
  MATCH (p:Patient {patientId: row.patient})
  SET
    p.bodyHeight_timestamps = [dt IN apoc.convert.fromJsonList(row.bodyHeight_timestamps) | datetime(dt)],
    p.bodyHeight_values = apoc.convert.fromJsonList(row.bodyHeight_values),
    p.bmi_timestamps = [dt IN apoc.convert.fromJsonList(row.bmi_timestamps) | datetime(dt)],
    p.bmi_values = apoc.convert.fromJsonList(row.bmi_values),
    p.respiratoryRate_timestamps = [dt IN apoc.convert.fromJsonList(row.respiratoryRate_timestamps) | datetime(dt)],
    p.respiratoryRate_values = apoc.convert.fromJsonList(row.respiratoryRate_values),
    p.heartRate_timestamps = [dt IN apoc.convert.fromJsonList(row.heartRate_timestamps) | datetime(dt)],
    p.heartRate_values = apoc.convert.fromJsonList(row.heartRate_values),
    p.bodyWeight_timestamps = [dt IN apoc.convert.fromJsonList(row.bodyWeight_timestamps) | datetime(dt)],
    p.bodyWeight_values = apoc.convert.fromJsonList(row.bodyWeight_values)
  ',
  {batchSize:1000, parallel:true}
);

CREATE INDEX patient_id IF NOT EXISTS FOR (p:Patient) ON (p.patientId);
CREATE INDEX patient_bodyHeight IF NOT EXISTS FOR (p:Patient) ON (p.bodyHeight_timestamps, p.bodyHeight_values);
CREATE INDEX patient_bmi IF NOT EXISTS FOR (p:Patient) ON (p.bmi_timestamps, p.bmi_values);
CREATE INDEX patient_respiratoryRate IF NOT EXISTS FOR (p:Patient) ON (p.respiratoryRate_timestamps, p.respiratoryRate_values);
CREATE INDEX patient_heartRate IF NOT EXISTS FOR (p:Patient) ON (p.heartRate_timestamps, p.heartRate_values);
CREATE INDEX patient_bodyWeight IF NOT EXISTS FOR (p:Patient) ON (p.bodyWeight_timestamps, p.bodyWeight_values);


MATCH (p1:Patient {gender: 'M'}), (p2:Patient)
WHERE (p2.birthdate.year - p1.birthdate.year) >= 25 AND (p2.birthdate.year - p1.birthdate.year) <= 35 
AND p1.last = p2.last
MERGE (p1)-[:FATHER_OF]->(p2);

MATCH (p1:Patient {gender: 'F'}), (p3:Patient)-[:FATHER_OF]->(p2:Patient)
WHERE (date(p2.birthdate).year - date(p1.birthdate).year) >= 25 
  AND (date(p2.birthdate).year - date(p1.birthdate).year) <= 35 
  AND p1.city = p2.city AND p2.city = p3.city 
MERGE (p1)-[:MOTHER_OF]->(p2);

MATCH (p1:Patient), (p2:Patient)
WHERE p1<>p2 and date(p2.birthdate).year = date(p1.birthdate).year AND p1.city = p2.city
MERGE (p1)-[:WAS_CLASSMATE_OF]->(p2);