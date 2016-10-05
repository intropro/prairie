package com.intropro.prairie.unit.hdfs.cmd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.shell.Command;
import org.apache.hadoop.fs.shell.CommandFactory;

import java.io.PrintStream;

/**
 * Created by presidentio on 9/6/16.
 */
public class CommandFactoryWithIO extends CommandFactory {

    private CommandFactory commandFactory;

    private PrintStream out;

    private PrintStream err;

    public CommandFactoryWithIO(CommandFactory commandFactory, PrintStream out, PrintStream err) {
        this.commandFactory = commandFactory;
        this.out = out;
        this.err = err;
    }

    @Override
    public void registerCommands(Class<?> registrarClass) {
        commandFactory.registerCommands(registrarClass);
    }

    @Override
    public void addClass(Class<? extends Command> cmdClass, String... names) {
        commandFactory.addClass(cmdClass, names);
    }

    @Override
    public void addObject(Command cmdObject, String... names) {
        commandFactory.addObject(cmdObject, names);
    }

    @Override
    public Command getInstance(String cmd) {
        Command command = commandFactory.getInstance(cmd);
        command.err = this.err;
        command.out = this.out;
        return command;
    }

    @Override
    public Command getInstance(String cmdName, Configuration conf) {
        Command command = commandFactory.getInstance(cmdName, conf);
        command.err = this.err;
        command.out = this.out;
        return command;
    }

    @Override
    public String[] getNames() {
        return commandFactory.getNames();
    }

}
