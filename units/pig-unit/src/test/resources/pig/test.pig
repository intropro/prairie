raw = LOAD '${INPUT_PATH}' USING PigStorage('|') AS (user, time, query, a);
raw = DISTINCT raw;
STORE raw INTO '${OUTPUT_PATH}' USING PigStorage('|');
