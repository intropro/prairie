package com.intropro.prairie.unit.oozie;

import com.intropro.prairie.unit.cmd.Command;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.List;

/**
 * Created by presidentio on 11/15/15.
 */
public class SaveArgsCommand implements Command {

    private List<String> args;

    @Override
    public int exec(List<String> args, Reader in, Writer out) throws InterruptedException, IOException {
        this.args = args;
        return 0;
    }

    @Override
    public boolean useInputStream() {
        return false;
    }

    public List<String> getArgs() {
        return args;
    }
}
