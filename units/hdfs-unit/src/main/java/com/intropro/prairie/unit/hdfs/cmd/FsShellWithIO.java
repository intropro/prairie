package com.intropro.prairie.unit.hdfs.cmd;

import org.apache.commons.io.output.WriterOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.shell.CommandFactory;
import org.apache.hadoop.fs.shell.FsCommand;

import java.io.*;

/**
 * Created by presidentio on 9/6/16.
 */
public class FsShellWithIO extends FsShell {

    private Reader in;
    private Writer out;

    public FsShellWithIO(Reader in, Writer out) {
        this.in = in;
        this.out = out;
    }

    public FsShellWithIO(Configuration conf, Reader in, Writer out) {
        super(conf);
        this.in = in;
        this.out = out;
    }

    @Override
    protected void init() throws IOException {
        super.init();
        this.commandFactory = new CommandFactoryWithIO(this.commandFactory, new PrintStream(new WriterOutputStream(out)),
                System.err);
    }

    @Override
    protected void registerCommands(CommandFactory factory) {
        factory.registerCommands(FsCommand.class);
    }
}
