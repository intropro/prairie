package com.intropro.prairie.unit.sshd.auth;

import org.apache.sshd.server.auth.password.PasswordAuthenticator;
import org.apache.sshd.server.auth.password.PasswordChangeRequiredException;
import org.apache.sshd.server.session.ServerSession;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by presidentio on 8/26/16.
 */
public class CollectionPasswordAuthenticator implements PasswordAuthenticator {

    private Map<String, String> users = new HashMap<>();

    @Override
    public boolean authenticate(String username, String password, ServerSession serverSession) throws PasswordChangeRequiredException {
        return password.equals(users.get(username));
    }

    public void addUser(String username, String password) {
        users.put(username, password);
    }
}
