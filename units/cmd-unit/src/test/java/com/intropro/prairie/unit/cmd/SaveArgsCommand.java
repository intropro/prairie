package com.intropro.prairie.unit.cmd;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.List;

/**
 * Created by presidentio on 11/15/15.
 */
public class SaveArgsCommand implements Command {

    private List<String> args;

    private int statusCode;

    public SaveArgsCommand(int statusCode) {
        this.statusCode = statusCode;
    }

    @Override
    public int exec(List<String> args, Reader in, Writer out) throws InterruptedException, IOException {
        this.args = args;
        return statusCode;
    }

    @Override
    public boolean useInputStream() {
        return false;
    }

    public List<String> getArgs() {
        return args;
    }
}
