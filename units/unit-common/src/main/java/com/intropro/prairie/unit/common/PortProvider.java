/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.intropro.prairie.unit.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;

/**
 * Created by Vitalii_Gergel on 2/10/2015.
 */
public class PortProvider {

    private static final Logger LOGGER = LogManager.getLogger(PortProvider.class);

    private static final int PORT_START = 9000;
    private static final int PORT_END = 10000;
    private static int CURRENT = PORT_START;

    public static synchronized int nextPort() {
        while (!isAvailable(++CURRENT)) {
            if (CURRENT == PORT_END) {
                throw new RuntimeException("All ports in user");
            }
        }
        return CURRENT;
    }

    private static boolean isAvailable(int port) {
        ServerSocket ss = null;
        DatagramSocket ds = null;
        try {
            ss = new ServerSocket(port);
            ss.setReuseAddress(true);
            ds = new DatagramSocket(port);
            ds.setReuseAddress(true);
            return true;
        } catch (IOException e) {
            LOGGER.debug(e.getMessage());
        } finally {
            if (ds != null) {
                ds.close();
            }

            if (ss != null) {
                try {
                    ss.close();
                } catch (IOException e) {
                    LOGGER.debug(e.getMessage());
                }
            }
        }

        return false;
    }

}
