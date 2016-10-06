#Prairie - Embedded Environment for Testing

###Overview
This framework can help you to write tests for your big data and not only products. It supports majority modern BigData technologies.

###Look closer
Framework consists of *units*. Unit is the service which responsible for one of the technologies(yarn, hive, etc).
Also they have useful methods which can be use in you tests.
Complex of units emulate cluster behavior.
Units can have dependencies to another units. For example yarn depends on hdfs.
You don't need to maintain this dependencies, they'll be created automatically.

####Example of hive unit with junit
```java

@RunWith(PrairieRunner.class)
public class Hive2UnitTest {

    @PrairieUnit
    private Hive2Unit hive2Unit;

    @Test
    public void testForDemo() throws Exception {
        hive2Unit.execute("create table prairie_test_table (id bigint, name string)");
        hive2Unit.execute("insert into prairie_test_table values (1, 'first')");
        List<Map<String, String>> tableContent = hive2Unit.executeQuery("select * from prairie_test_table");
        Map<String, String> expectedRaw = new HashMap<>();
        expectedRaw.put("prairie_test_table.id", "1");
        expectedRaw.put("prairie_test_table.name", "first");
        Assert.assertEquals(expectedRaw, tableContent.get(0));
    }
}
```
More examples you can find in test directories for each unit:

[Hdfs](https://github.com/intropro/prairie/blob/master/units/hdfs-unit/src/test/java/com/intropro/prairie/unit/hdfs/HdfsUnitTest.java),
[Yarn](https://github.com/intropro/prairie/blob/master/units/yarn-unit/src/test/java/com/intropro/prairie/unit/yarn/YarnUnitTest.java),
[Hive2](https://github.com/intropro/prairie/blob/master/units/hive2-unit/src/test/java/com/intropro/prairie/unit/hive2/Hive2UnitTest.java),
[Pig](https://github.com/intropro/prairie/blob/master/units/pig-unit/src/test/java/com/intropro/prairie/unit/pig/PigUnitTest.java),
[Oozie](https://github.com/intropro/prairie/blob/master/units/oozie-unit/src/test/java/com/intropro/prairie/unit/oozie/OozieUnitTest.java),
[Flume](https://github.com/intropro/prairie/blob/master/units/flume-unit/src/test/java/com/intropro/prairie/unit/flume/FlumeUnitTest.java),
[Zookeeper](https://github.com/intropro/prairie/blob/master/units/zookeeper-unit/src/test/java/com/intropro/prairie/unit/zookeeper/ZookeeperUnitTest.java),
[Kafka](https://github.com/intropro/prairie/blob/master/units/kafka-unit/src/test/java/com/intropro/prairie/unit/kafka/KafkaUnitTest.java)
[Cmd](https://github.com/intropro/prairie/blob/master/units/cmd-unit/src/test/java/com/intropro/prairie/unit/cmd/CmdUnitTest.java)
[HBase](https://github.com/intropro/prairie/blob/master/units/cmd-unit/src/test/java/com/intropro/prairie/unit/hbase/HBaseUnitTest.java)
[Cassandra](https://github.com/intropro/prairie/blob/master/units/cmd-unit/src/test/java/com/intropro/prairie/unit/cassandra/CassandraUnitTest.java)

####Example of hive unit
```java
public class Hive2UnitDemo {

    @PrairieUnit
    private Hive2Unit hive2Unit;

    public void runDemo() throws SQLException {
        hive2Unit.execute("create table prairie_test_table (id bigint, name string)");
        hive2Unit.execute("insert into prairie_test_table values (1, 'first')");
        List<Map<String, String>> tableContent = hive2Unit.executeQuery("select * from prairie_test_table");
        for (Map<String, String> row : tableContent) {
            System.out.println(row);
        }
    }

    public static void main(String[] args) throws PrairieException, SQLException {
        DependencyResolver dependencyResolver = new DependencyResolver();
        Hive2UnitDemo hive2UnitDemo = new Hive2UnitDemo();
        dependencyResolver.resolve(hive2UnitDemo);
        hive2UnitDemo.runDemo();
        dependencyResolver.destroy(hive2UnitDemo);
    }
}
```

###Maven
To use one of units you need to put dependency to you pom.xml 
```xml
<dependency>
    <groupId>com.intropro.prairie</groupId>
    <artifactId>${unit.id}</artifactId>
    <version>1.1.0</version>
</dependency>
```
and replace unit.id with data from table below

|Unit ID|Unit Version|
|-------|------------|
|hdfs-unit|1.1.1|
|yarn-unit|1.1.1|
|hive2-unit|1.1.1|
|oozie-unit|1.1.1|
|flume-unit|1.1.1|
|zookeeper-unit|1.1.1|
|kafka-unit|1.1.1|
|pig-unit|1.1.1|
|cmd-unit|1.1.1|
|hbase-unit|1.1.1|
|cassandra-unit|1.1.1|

###Technologies
<img src="https://upload.wikimedia.org/wikipedia/commons/thumb/0/0e/Hadoop_logo.svg/664px-Hadoop_logo.svg.png" height="100">
<img src="https://upload.wikimedia.org/wikipedia/commons/thumb/b/bb/Apache_Hive_logo.svg/2000px-Apache_Hive_logo.svg.png" height="100">
<img src="https://cwiki.apache.org/confluence/download/attachments/30737784/oozie_282x1178.png?version=1&modificationDate=1349284899000&api=v2" height="100">
<img src="https://flume.apache.org/_static/flume-logo.png" height="100">
<img src="https://s3.amazonaws.com/files.dezyre.com/images/Tutorials/zookeeper_logo.png" height="100">
<img src="https://upload.wikimedia.org/wikipedia/commons/f/f7/Apache_kafka.png" height="100">
<img src="https://www.mapr.com/sites/default/files/pig-image.png" height="100">
<img src="http://hbase.apache.org/images/hbase_logo.png" height="100">
<img src="http://cassandra.apache.org/img/cassandra_logo.png" height="100">

|Technology|Version|
|----|-------|
|Hadoop|2.2.0 or higher|
|Hive|1.0.1 or higher|
|Oozie|4.0.0 or higher|
|Flume|1.5.0 or higher|
|Zookeeper|3.4.5 or higher|
|Kafka|0.8.2.2 or higher|
|Pig|0.12.0 or higher|
|HBase|1.2.0|
|Cassandra|3.9|

###Supported Platforms
|Platform|Version|
|----|-------|
|CDH|5.2.0 or higher|
|HDP|2.2.0 or higher|

###JUnit
Framework have junit runner *PrairieRunner* which initialize all units, and resolve inner dependencies.
To use it you need to put dependency to your pom.xml:
```xml
<dependency>
    <groupId>com.intropro.prairie</groupId>
    <artifactId>junit-runner</artifactId>
    <version>1.1.1</version>
</dependency>
```

Licensing
=========

[![][license img]][license]

This software is licensed under the terms in the file named "LICENSE" in this directory.


[license]:LICENSE
[license img]:https://img.shields.io/badge/license-Apache%202.0-brightgreen.svg