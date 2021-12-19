package com.intropro.prairie.unit.cmd;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.Socket;
import java.util.Arrays;

/**
 * Created by presidentio on 11/26/15.
 */
public class CommandClient {

    private static final Logger LOGGER = LogManager.getLogger(CommandsServer.class);

    @Parameter(names = "-h")
    private String host;

    @Parameter(names = "-p")
    private int port;

    @Parameter(names = "-c")
    private String command;

    private String[] args;

    private int exitStatus;

    public static void main(String[] args) throws IOException, InterruptedException {
        CommandClient commandClient = new CommandClient();
        JCommander jCommander = new JCommander(commandClient);
        jCommander.parse(Arrays.copyOfRange(args, 0, 6));
        commandClient.setArgs(Arrays.copyOfRange(args, 6, args.length));
        System.exit(commandClient.exec());
    }

    public int exec() throws IOException, InterruptedException {
        System.err.println("Executing command " + command + " on " + host + ":" + port + " with args: " + Arrays.toString(args));
        Socket socket = new Socket(host, port);
        socket.getOutputStream().write((command + "\n").getBytes());
        socket.getOutputStream().write((StringUtils.join(args, ' ') + "\n").getBytes());
        StreamRedirect inStreamRedirect = new StreamRedirect(System.in, socket.getOutputStream());
        inStreamRedirect.start();
        StreamRedirect outStreamRedirect = new StreamRedirect(socket.getInputStream(), System.out) {
            @Override
            protected void handleLine(String line) throws IOException {
                if (line.startsWith(CommandProcessor.FIRST_LINE_MARKER)) {
                    super.write(line.replace(CommandProcessor.FIRST_LINE_MARKER, ""));
                } else {
                    if (isEof()) {
                        exitStatus = Integer.valueOf(line);
                    } else {
                        super.write("\n" + line);
                    }
                }
            }
        };
        outStreamRedirect.start();
        inStreamRedirect.join();
        socket.shutdownOutput();
        outStreamRedirect.join();
        System.err.println("Finished command " + command + " on " + host + ":" + port);
        return exitStatus;
    }

    public CommandClient setHost(String host) {
        this.host = host;
        return this;
    }

    public CommandClient setPort(int port) {
        this.port = port;
        return this;
    }

    public CommandClient setCommand(String command) {
        this.command = command;
        return this;
    }

    public CommandClient setArgs(String[] args) {
        this.args = args;
        return this;
    }
}
