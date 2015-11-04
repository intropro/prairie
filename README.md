#Prairie - BigData Embedded Cluster for Testing

###Overview
This framework can help you to write tests for your big data products. It supports majority modern BigData technologies.

###Look closer
Framework consists of *units*. Unit is the service which responsible for one of the technologies(yarn, hive, etc).
Also they have useful methods which can be use in you tests.
Complex of units emulate cluster behavior.
Units can have dependencies to another units. For example yarn depends on hdfs.
You don't need to maintain this dependencies, they'll be create automatically.

###JUnit
Framework have junit runner *BigDataTestRunner* which initialize all units, and resolve inner dependencies.
To use it you need to put dependency to your pom.xml:
```xml
<dependency>
    <groupId>com.intropro.prairie</groupId>
    <artifactId>junit-runner</artifactId>
    <version>1.0.0</version>
</dependency>
```

####Example of hive unit with junit
```java

@RunWith(BigDataTestRunner.class)
public class Hive2UnitTest {

    @BigDataUnit
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

####Example of hive unit
```java
public class Hive2UnitDemo {

    @BigDataUnit
    private Hive2Unit hive2Unit;

    public void runDemo() throws SQLException {
        hive2Unit.execute("create table prairie_test_table (id bigint, name string)");
        hive2Unit.execute("insert into prairie_test_table values (1, 'first')");
        List<Map<String, String>> tableContent = hive2Unit.executeQuery("select * from prairie_test_table");
        for (Map<String, String> row : tableContent) {
            System.out.println(row);
        }
    }

    public static void main(String[] args) throws BigDataTestFrameworkException, SQLException {
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
    <version>${unit.version}</version>
</dependency>
```
and replace unit.version and unit.id with data from table below

|Unit ID|Unit Version|
|-------|------------|
|hdfs-unit|1.0.0|
|yarn-unit|1.0.0|
|hive2-unit|1.0.0|
|oozie-unit|1.0.0|
|flume-unit|1.0.0|
|zookeeper-unit|1.0.0|
|kafka-unit|1.0.0|
|pig-unit|1.0.0|
###Technologies
|Technology Name|Technology Version|
|----|-------|
|Hdfs|2.7.1|
|Yarn|2.7.1|
|Hive|1.2.1|
|Oozie|4.2.0|
|Flume|1.6.0|
|Zookeeper|3.4.6|
|Kafka|0.8.2.2|
|Pig|0.15.0|

Licensing
=========

[![][license img]][license]

This software is licensed under the terms in the file named "LICENSE" in this directory.


[license]:LICENSE
[license img]:https://img.shields.io/badge/license-Apache%202.0-brightgreen.svg