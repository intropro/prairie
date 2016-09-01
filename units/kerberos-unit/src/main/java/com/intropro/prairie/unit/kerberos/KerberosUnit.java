package com.intropro.prairie.unit.kerberos;

import com.intropro.prairie.unit.common.BaseUnit;
import com.intropro.prairie.unit.common.PortProvider;
import com.intropro.prairie.unit.common.exception.DestroyUnitException;
import com.intropro.prairie.unit.common.exception.InitUnitException;
import com.intropro.prairie.unit.kerberos.exception.KerberosException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by presidentio on 8/26/16.
 */
public class KerberosUnit extends BaseUnit {

    private static final Logger LOGGER = LogManager.getLogger(KerberosUnit.class);

    private KerberosServer kerberosServer;
    private KerberosUserManager kerberosUserManager;
    private String realm = "PRAIRIE";

    public KerberosUnit() {
        super("kerberos");
    }

    @Override
    protected void init() throws InitUnitException {
        kerberosServer = new KerberosServer(getTmpDir().resolve("kerberos-server"), realm, PortProvider.nextPort());
        kerberosUserManager = new KerberosUserManagerImpl(kerberosServer, getTmpDir().resolve("keytabs"));
        try {
            kerberosServer.init();
        } catch (KerberosException e) {
            throw new InitUnitException("Failed to init kerberos server", e);
        }
    }

    public KerberosUserManager getKerberosUserManager() {
        return kerberosUserManager;
    }

    public String getRealm() {
        return realm;
    }

    public String getKdcHost() {
        return kerberosServer.getHost();
    }

    public int getKdcPort() {
        return kerberosServer.getPort();
    }

    @Override
    protected void destroy() throws DestroyUnitException {
        try {
            kerberosServer.destroy();
        } catch (KerberosException e) {
            throw new DestroyUnitException("Failed to destroy kerberos server", e);
        }
    }

}
