package com.cosmian.cloudproof_demo.injector;

import java.io.Closeable;
import java.io.Serializable;

import com.cosmian.cloudproof_demo.AppException;

public interface LineReader extends Closeable, Serializable {

    /**
     * Reads the next line
     * 
     * @return the line or null if no more lines can be read
     * @throws AppException
     */
    String readNext() throws AppException;

}
