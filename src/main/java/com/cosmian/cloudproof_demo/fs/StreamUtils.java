package com.cosmian.cloudproof_demo.fs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public final class StreamUtils {

    public static byte[] read_all_bytes(InputStream inputStream) throws IOException {
        final int BUFFER_LENGTH = 4096;
        byte[] buffer = new byte[BUFFER_LENGTH];
        int readLen;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        while ((readLen = inputStream.read(buffer, 0, BUFFER_LENGTH)) != -1)
            outputStream.write(buffer, 0, readLen);
        return outputStream.toByteArray();
    }

}
