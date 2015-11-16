package com.intropro.prairie.unit.cmd;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.List;

/**
 * Created by presidentio on 11/14/15.
 */
public class MirrorCommand implements Command {

    @Override
    public int exec(List<String> args, Reader in, Writer out) throws InterruptedException, IOException {
        BufferedReader bufferedReader = new BufferedReader(in);
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            System.out.println(line);
            out.write(line);
            out.write("\n");
            out.flush();
        }
        return 125;
    }

    @Override
    public boolean useInputStream() {
        return true;
    }

}
