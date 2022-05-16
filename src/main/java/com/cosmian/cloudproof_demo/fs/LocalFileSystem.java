package com.cosmian.cloudproof_demo.fs;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

import com.cosmian.cloudproof_demo.AppException;

public class LocalFileSystem implements AppFileSystem {

    public LocalFileSystem() {
    }

    @Override
    public void writeFile(String filePath, byte[] bytes) throws AppException {
        Path path = Paths.get(filePath);
        try (OutputStream os = new FileOutputStream(path.toFile());) {
            os.write(bytes);
            os.flush();
        } catch (IOException e) {
            throw new AppException("failed writing: " + path.toString() + ": " + e.getMessage(), e);
        }

    }

    public byte[] readFile(File file) throws AppException {
        try (InputStream is = new FileInputStream(file);) {
            return read_all_bytes(is);
        } catch (IOException e) {
            throw new AppException("failed reading the file at: " + file.getAbsolutePath() + ": " + e.getMessage(), e);
        }
    }

    @Override
    public byte[] readFile(String filePath) throws AppException {
        Path path = Paths.get(filePath);
        return readFile(path.toFile());
    }

    @Override
    public OutputStream getOutputStream(String filePath, boolean overwrite) throws AppException {
        Path path = Paths.get(filePath);
        try {
            return new FileOutputStream(path.toFile(), !overwrite);
        } catch (FileNotFoundException e) {
            throw new AppException("failed writing: " + path.toString() + ": file not found", e);
        }
    }

    public static byte[] read_all_bytes(InputStream inputStream) throws IOException {
        final int BUFFER_LENGTH = 4096;
        byte[] buffer = new byte[BUFFER_LENGTH];
        int readLen;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        while ((readLen = inputStream.read(buffer, 0, BUFFER_LENGTH)) != -1)
            outputStream.write(buffer, 0, readLen);
        return outputStream.toByteArray();
    }

    @Override
    public Iterator<String> listFiles(String directoryPath) throws AppException {
        try {
            return Files.list(Paths.get(directoryPath)).map(p -> p.toAbsolutePath().toString()).iterator();
        } catch (IOException e) {
            throw new AppException("failed listing the files at: " + directoryPath + ": " + e.getMessage());
        }
    }

    @Override
    public boolean isDirectory(String directoryPath) throws AppException {
        return Files.isDirectory(Paths.get(directoryPath));
    }

    @Override
    public InputStream getInputStream(String filePath) throws AppException {
        Path path = Paths.get(filePath);
        try {
            return new FileInputStream(path.toFile());
        } catch (FileNotFoundException e) {
            throw new AppException("failed reading: " + path.toString() + ": file not found", e);
        }
    }

    @Override
    public String getPath(String directoryPath, String filename) throws AppException {
        return Paths.get(directoryPath, filename).toAbsolutePath().toString();
    }

}
