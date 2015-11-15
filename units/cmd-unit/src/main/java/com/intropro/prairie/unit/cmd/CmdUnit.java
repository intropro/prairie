package com.intropro.prairie.unit.cmd;

import com.intropro.prairie.unit.common.BaseUnit;
import com.intropro.prairie.unit.common.PortProvider;
import com.intropro.prairie.unit.common.exception.DestroyUnitException;
import com.intropro.prairie.unit.common.exception.InitUnitException;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.concurrent.TimeUnit;

/**
 * Created by presidentio on 11/13/15.
 */
public class CmdUnit extends BaseUnit {

    private static final String PATH_SEPARATOR = ":";

    private String path = "";
    private CommandsServer commandsServer;

    private int port;
    private String host = "localhost";

    private String mockCommand;

    private String proxyCommandPath;

    public CmdUnit() {
        super("cmd");
    }

    @Override
    protected void init() throws InitUnitException {
        port = PortProvider.nextPort();
        commandsServer = new CommandsServer(port);
        commandsServer.start();
        try {
            deployProxyCommand();
            mockCommand = IOUtils.toString(CmdUnit.class.getClassLoader().getResourceAsStream("mock.sh"));
        } catch (IOException e) {
            throw new InitUnitException("Failed to load proxy command from resources", e);
        }
    }

    @Override
    protected void destroy() throws DestroyUnitException {
        commandsServer.interrupt();
    }

    public String declare(String alias, Command command) throws IOException {
        if (!path.isEmpty()) {
            path += PATH_SEPARATOR;
        }
        path += deployCommand(alias, command);
        return path;
    }

    private String deployCommand(String alias, Command command) throws IOException {
        commandsServer.addCommand(alias, command);
        String commandBody = String.format(mockCommand, proxyCommandPath, alias);
        File commandFile = new File(getTmpDir().toFile(), alias);
        FileWriter commandFileWriter = new FileWriter(commandFile);
        commandFileWriter.write(commandBody);
        commandFileWriter.close();
        commandFile.setExecutable(true);
        return commandFile.getAbsolutePath();
    }

    private void deployProxyCommand() throws IOException {
        String proxyCommand = IOUtils.toString(
                CmdUnit.class.getClassLoader().getResourceAsStream("proxy.sh"));
        File proxyCommandFile = new File(getTmpDir().toFile(), "proxy.sh");
        Writer proxyCommandWriter = new FileWriter(proxyCommandFile);
        proxyCommandWriter.write(String.format(proxyCommand, host, port));
        proxyCommandWriter.close();
        proxyCommandPath = proxyCommandFile.getAbsolutePath();
        proxyCommandFile.setExecutable(true);
    }

    public static void main(String[] args) throws InitUnitException, InterruptedException, DestroyUnitException, IOException {
        CmdUnit cmdUnit = new CmdUnit();
        cmdUnit.start();
        cmdUnit.declare("a.sh", new MirrorCommand());
        Thread.sleep(TimeUnit.MINUTES.toMillis(10));
        cmdUnit.stop();
    }
}
