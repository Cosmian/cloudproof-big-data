package com.cosmian.cloudproof_demo.fs;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

import com.cosmian.cloudproof_demo.AppException;

public interface AppFileSystem {

    public void writeFile(String filePath, byte[] bytes) throws AppException;

    public byte[] readFile(String filePath) throws AppException;

    public OutputStream getOutputStream(String filePath, boolean overwrite) throws AppException;

    public InputStream getInputStream(String filePath) throws AppException;

    public Iterator<String> listFiles(String directoryPath) throws AppException;

    public boolean isDirectory(String directoryPath) throws AppException;

    public String getPath(String directoryPath, String filename) throws AppException;
}
