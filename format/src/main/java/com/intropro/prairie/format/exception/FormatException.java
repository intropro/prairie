package com.intropro.prairie.format.exception;

/**
 * Created by presidentio on 12/12/15.
 */
public class FormatException extends Exception {

    public FormatException() {
        super();
    }

    public FormatException(String message) {
        super(message);
    }

    public FormatException(String message, Throwable cause) {
        super(message, cause);
    }

    public FormatException(Throwable cause) {
        super(cause);
    }

    protected FormatException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
