#!/bin/bash

MORPH="cabal run -v0 morph --"

echo "=== Table Replacement Test ==="
cat > table_test.sql <<EOF
SELECT * FROM old_tab;
SELECT * FROM sch.old_tab;
SELECT * FROM cat.sch.old_tab;
EOF
$MORPH replace-table --file table_test.sql --from old_tab --to new_tab --default-catalog cat --default-schema sch
echo

echo "=== Field Replacement Test (Success) ==="
cat > field_test.sql <<EOF
SELECT col1 FROM tab1;
SELECT tab1.col1 FROM tab1;
SELECT t.col1 FROM tab1 AS t;
EOF
$MORPH replace-field --file field_test.sql --from cat.sch.tab1.col1 --to cat.sch.tab1.col1_new --default-catalog cat --default-schema sch
echo

echo "=== Field Replacement Test (Ambiguity Error) ==="
cat > ambiguity_test.sql <<EOF
SELECT col1 FROM tab1, tab2;
EOF
$MORPH replace-field --file ambiguity_test.sql --from cat.sch.tab1.col1 --to cat.sch.tab1.col1_new --default-catalog cat --default-schema sch 2>&1
echo
