package com.intropro.prairie.unit.kerberos;

import java.io.File;

/**
 * Created by presidentio on 8/30/16.
 */
public class KerberosUser {

    private String username;

    private String password;

    private File keytab;

    private String principla;

    public KerberosUser(String username, String password, File keytab, String principla) {
        this.username = username;
        this.password = password;
        this.keytab = keytab;
        this.principla = principla;
    }

    public KerberosUser(String password, String username) {
        this.password = password;
        this.username = username;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public File getKeytab() {
        return keytab;
    }

    public void setKeytab(File keytab) {
        this.keytab = keytab;
    }

    public String getPrincipal() {
        return principla;
    }

    public void setPrincipla(String principla) {
        this.principla = principla;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KerberosUser that = (KerberosUser) o;

        if (principla != null ? !principla.equals(that.principla) : that.principla != null) return false;
        if (username != null ? !username.equals(that.username) : that.username != null) return false;
        if (password != null ? !password.equals(that.password) : that.password != null) return false;
        return keytab != null ? keytab.equals(that.keytab) : that.keytab == null;

    }

    @Override
    public int hashCode() {
        int result = principla != null ? principla.hashCode() : 0;
        result = 31 * result + (username != null ? username.hashCode() : 0);
        result = 31 * result + (password != null ? password.hashCode() : 0);
        result = 31 * result + (keytab != null ? keytab.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "KerberosUser{" +
                "username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", keytab=" + keytab +
                ", principla='" + principla + '\'' +
                '}';
    }
}
