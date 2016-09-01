package com.intropro.prairie.unit.kerberos.exception;

import com.intropro.prairie.unit.common.exception.PrairieException;

/**
 * Created by presidentio on 8/29/16.
 */
public class KerberosException extends PrairieException {

    public KerberosException() {
        super();
    }

    public KerberosException(String message) {
        super(message);
    }

    public KerberosException(String message, Throwable cause) {
        super(message, cause);
    }

    public KerberosException(Throwable cause) {
        super(cause);
    }
}
