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
        boolean first = true;
        while ((line = bufferedReader.readLine()) != null) {
            if(!first){
                out.write("\n");
            }
            out.write(line);
            first = false;
        }
        return 0;
    }

}
