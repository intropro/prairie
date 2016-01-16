package com.intropro.prairie.unit.hive2;

import com.intropro.prairie.unit.common.DependencyResolver;
import com.intropro.prairie.unit.common.annotation.PrairieUnit;
import com.intropro.prairie.unit.common.exception.PrairieException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Created by presidentio on 10/22/15.
 */
public class Hive2UnitDemo {

    @PrairieUnit
    private Hive2Unit hive2Unit;

    public void runDemo() throws SQLException, IOException {
        Hive2UnitClient client = hive2Unit.createClient();
        client.execute("create table prairie_test_table (id bigint, name string)");
        client.execute("insert into prairie_test_table values (1, 'first')");
        List<Map<String, String>> tableContent = client.executeQuery("select * from prairie_test_table");
        for (Map<String, String> row : tableContent) {
            System.out.println(row);
        }
    }

    public static void main(String[] args) throws PrairieException, SQLException, IOException {
        DependencyResolver dependencyResolver = new DependencyResolver();
        Hive2UnitDemo hive2UnitDemo = new Hive2UnitDemo();
        dependencyResolver.resolve(hive2UnitDemo);
        hive2UnitDemo.runDemo();
        dependencyResolver.destroy(hive2UnitDemo);
    }
}
