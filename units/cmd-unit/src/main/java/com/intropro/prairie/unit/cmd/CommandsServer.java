package com.intropro.prairie.unit.cmd;

import com.intropro.prairie.unit.common.exception.DestroyUnitException;
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

    private ServerSocket serverSocket;

    public CommandsServer(int port) {
        this.port = port;
    }

    public void waitStart() throws InterruptedException {
        while (serverSocket == null || !serverSocket.isBound()) {
            Thread.sleep(10);
        }
    }

    @Override
    public void run() {
        try {
            serverSocket = new ServerSocket(port);
            try {
                while (!isInterrupted()) {
                    Socket socket = serverSocket.accept();
                    CommandProcessor commandProcessor = new CommandProcessor(socket, this);
                    commandProcessor.start();
                }
            } catch (IOException e) {
                LOGGER.error("Server socket error or socket closed: " + e.getClass() + ": " + e.getMessage());
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

    public void close() throws DestroyUnitException {
        interrupt();
        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                throw new DestroyUnitException(e);
            }
        }
    }
}
