package com.intropro.prairie.unit.sshd.auth;

import org.apache.sshd.common.util.buffer.Buffer;
import org.apache.sshd.server.auth.UserAuth;
import org.apache.sshd.server.auth.gss.UserAuthGSSFactory;
import org.apache.sshd.server.session.ServerSession;

import java.util.List;

/**
 * Created by presidentio on 9/2/16.
 */
public class MockedGSSAuth implements UserAuth {

    private List<String> users;

    private ServerSession session;

    private String username;

    public MockedGSSAuth(List<String> users) {
        this.users = users;
    }

    @Override
    public Boolean auth(ServerSession session, String username, String service, Buffer buffer) throws Exception {
        this.session = session;
        this.username = username;
        return users.contains(username);
    }

    @Override
    public Boolean next(Buffer buffer) throws Exception {
        return true;
    }

    @Override
    public void destroy() {

    }

    @Override
    public ServerSession getSession() {
        return session;
    }

    @Override
    public String getName() {
        return UserAuthGSSFactory.NAME;
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public ServerSession getServerSession() {
        return session;
    }
}
