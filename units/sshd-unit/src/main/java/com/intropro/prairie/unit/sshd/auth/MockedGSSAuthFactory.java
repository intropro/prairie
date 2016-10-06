package com.intropro.prairie.unit.sshd.auth;

import org.apache.sshd.server.auth.UserAuth;
import org.apache.sshd.server.auth.UserAuthFactory;
import org.apache.sshd.server.auth.gss.UserAuthGSSFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by presidentio on 9/2/16.
 */
public class MockedGSSAuthFactory implements UserAuthFactory {

    private List<String> users = new ArrayList<>();

    @Override
    public UserAuth create() {
        return new MockedGSSAuth(users);
    }

    @Override
    public String getName() {
        return UserAuthGSSFactory.NAME;
    }

    public void addUser(String username) {
        users.add(username);
    }
}
