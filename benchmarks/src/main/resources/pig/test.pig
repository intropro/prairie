raw = LOAD '${INPUT_PATH}' USING PigStorage('|') AS (user, time, query, a);
STORE raw INTO '${OUTPUT_PATH}' USING PigStorage('|');
