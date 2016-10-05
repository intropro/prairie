package com.intropro.prairie.unit.hdfs.cmd;

import com.intropro.prairie.unit.cmd.Command;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.List;

/**
 * Created by presidentio on 8/19/16.
 */
public class HdfsShell implements Command {

    private static final Logger LOGGER = LogManager.getLogger(HdfsShell.class);

    private Configuration configuration;

    public HdfsShell(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public int exec(List<String> args, Reader in, Writer out) throws InterruptedException, IOException {
        FsShell fsShell = new FsShellWithIO(configuration, in, out);
        try {
            return fsShell.run(args.subList(1, args.size()).toArray(new String[args.size() - 1]));
        } catch (Exception e) {
            LOGGER.error("Failed to execute hdfs shell with args: " + args, e);
            return 1;
        }
    }

    @Override
    public boolean useInputStream() {
        return false;
    }

}
