package com.cosmian.cloudproof_demo;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

public class LocalResource {
    public static byte[] read_all_bytes(InputStream inputStream) throws IOException {
        final int BUFFER_LENGTH = 4096;
        byte[] buffer = new byte[BUFFER_LENGTH];
        int readLen;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        while ((readLen = inputStream.read(buffer, 0, BUFFER_LENGTH)) != -1)
            outputStream.write(buffer, 0, readLen);
        return outputStream.toByteArray();
    }

    public static String load_resource(String resource_name) throws IOException {
        try (InputStream is = LocalResource.class.getClassLoader().getResourceAsStream(resource_name)) {
            return new String(read_all_bytes(is), StandardCharsets.UTF_8);
        }
    }

    public static String load_file_string(File file) throws IOException {
        return new String(load_file_bytes(file), StandardCharsets.UTF_8);
    }

    public static byte[] load_file_bytes(File file) throws IOException {
        try (InputStream is = new FileInputStream(file);) {
            return read_all_bytes(is);
        }
    }

    public static void write_file(String parent, String fileName, byte[] content) throws IOException {
        Path path = Paths.get(parent, fileName);
        try (OutputStream os = new FileOutputStream(path.toFile());) {
            os.write(content);
            os.flush();
        }
    }

    public static void write_resource(String resource_name, byte[] bytes) throws IOException {
        String parentDir = LocalResource.class.getClassLoader().getResource(".").getFile();
        System.out.println("PARENT DIR " + parentDir);
        String path = Paths.get(parentDir, resource_name).toString();
        try (OutputStream os = new FileOutputStream(path)) {
            os.write(bytes);
            os.flush();
        }
    }
}
