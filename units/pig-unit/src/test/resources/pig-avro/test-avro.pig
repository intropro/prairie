input1 = LOAD '${INPUT_PATH1}' using org.apache.pig.piggybank.storage.avro.AvroStorage('{"schema": {"type":"record","name":"Test","namespace":"com.intropro.prairie","fields":[{"name":"field1","type":["string","null"]},{"name":"field2","type":["string","null"]},{"name":"field3","type":["string","null"]},{"name":"FIELD","type":{"type":"map","values":"string"}}]}}');
input2 = LOAD '${INPUT_PATH2}' using PigStorage('|') AS (field21:chararray,field22:chararray);
tmp = join input1 by FIELD#'field4.field1', input2 by field21;
output1 = FOREACH tmp GENERATE field1, FIELD#'field4.field1', field22;
store output1 into '${OUTPUT_PATH}' USING PigStorage('|');