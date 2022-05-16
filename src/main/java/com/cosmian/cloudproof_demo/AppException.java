package com.cosmian.cloudproof_demo;

/**
 * An Exception thrown by the cosmian API calls
 */
public class AppException extends Exception {
    public AppException(String message, Throwable t) {
        super(message, t);
    }

    public AppException(String message) {
        super(message);
    }
}
