package com.intropro.prairie.unit.cmd;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.List;

/**
 * Created by presidentio on 11/13/15.
 */
public interface Command {

    int exec(List<String> args, Reader in, Writer out) throws InterruptedException, IOException;

}
