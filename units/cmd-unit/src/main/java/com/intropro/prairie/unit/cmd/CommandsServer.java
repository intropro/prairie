package com.intropro.prairie.unit.cmd;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by presidentio on 11/13/15.
 */
public class CommandsServer extends Thread implements CommandProvider {

    private static final Logger LOGGER = LogManager.getLogger(CommandsServer.class);

    private int port;

    private Map<String, Command> commands = new HashMap<>();

    private List<CommandProcessor> commandProcessors = new ArrayList<>();

    public CommandsServer(int port) {
        this.port = port;
    }

    @Override
    public void run() {
        try {
            ServerSocket serverSocket = new ServerSocket(port);
            try {
                while (!isInterrupted()) {
                    Socket socket = serverSocket.accept();
                    CommandProcessor commandProcessor = new CommandProcessor(socket, this);
                    commandProcessor.start();
                }
            } catch (IOException e) {
                LOGGER.error("Server socket error", e);
            }
            for (CommandProcessor commandProcessor : commandProcessors) {
                commandProcessor.interrupt();
            }
            serverSocket.close();
        } catch (IOException e) {
            LOGGER.error("Server socket error", e);
        }
    }

    public void addCommand(String alias, Command command) {
        commands.put(alias, command);
    }

    @Override
    public Command getCommand(String name) {
        return commands.get(name);
    }
}
