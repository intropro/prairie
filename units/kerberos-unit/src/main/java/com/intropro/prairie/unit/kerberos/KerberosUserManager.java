package com.intropro.prairie.unit.kerberos;

import com.intropro.prairie.unit.kerberos.exception.KerberosException;

/**
 * Created by presidentio on 8/30/16.
 */
public interface KerberosUserManager {

    KerberosUser addUser(String username, String password) throws KerberosException;
    KerberosUser addService(String username, String password) throws KerberosException;

    KerberosUser getUser(String username);

}
