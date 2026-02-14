-- Table replacement
SELECT * FROM old_tab;
SELECT * FROM sch.old_tab;
SELECT * FROM cat.sch.old_tab;
UPDATE old_tab SET col = 1;
DELETE FROM old_tab WHERE col = 1;
INSERT INTO old_tab SELECT * FROM other;

-- Field replacement
SELECT col1 FROM tab1;
SELECT tab1.col1 FROM tab1;
SELECT t.col1 FROM tab1 AS t;
SELECT col1 FROM tab1, tab2;
SELECT tab1.col1 FROM tab1, tab2;
