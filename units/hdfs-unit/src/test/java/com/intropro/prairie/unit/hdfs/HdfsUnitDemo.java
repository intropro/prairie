package com.intropro.prairie.unit.hdfs;

import com.intropro.prairie.unit.common.DependencyResolver;
import com.intropro.prairie.unit.common.annotation.PrairieUnit;
import com.intropro.prairie.unit.common.exception.PrairieException;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by presidentio on 10/22/15.
 */
public class HdfsUnitDemo {

    @PrairieUnit
    private HdfsUnit hdfsUnit;

    public void runDemo() throws IOException {
        hdfsUnit.getFileSystem().mkdirs(new Path("/data"));
    }

    public static void main(String[] args) throws PrairieException, IOException {
        DependencyResolver dependencyResolver = new DependencyResolver();
        HdfsUnitDemo hdfsUnitDemo = new HdfsUnitDemo();
        dependencyResolver.resolve(hdfsUnitDemo);
        hdfsUnitDemo.runDemo();
        dependencyResolver.destroy(hdfsUnitDemo);
    }
}
