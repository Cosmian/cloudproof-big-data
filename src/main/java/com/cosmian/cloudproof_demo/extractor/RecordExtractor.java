package com.cosmian.cloudproof_demo.extractor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

import org.json.JSONObject;

import com.cosmian.CosmianException;
import com.cosmian.cloudproof_demo.AppException;
import com.cosmian.jna.FfiException;
import com.cosmian.jna.cover_crypt.DecryptedHeader;
import com.cosmian.jna.cover_crypt.Ffi;

public class RecordExtractor {

    private static final Logger logger = Logger.getLogger(RecordExtractor.class.getName());

    /**
     * Decrypt the record
     */
    public static byte[] process(int decryptionCache, byte[] uid, byte[] encryptedBytes) throws AppException {

        ByteArrayInputStream bai = new ByteArrayInputStream(encryptedBytes);

        try {
            // decrypt the common part
            byte[] commonPart = decryptPart(decryptionCache, uid, bai);
            JSONObject json = new JSONObject(new String(commonPart, StandardCharsets.UTF_8));

            // try decrypting the marketing part
            try {
                byte[] mkgPart = decryptPart(decryptionCache, uid, bai);
                JSONObject mkgJson = new JSONObject(new String(mkgPart, StandardCharsets.UTF_8));
                mkgJson.keys().forEachRemaining(k -> json.put(k, mkgJson.getString(k)));
            } catch (AppException e) {
                // no right ignore
                logger.finer(() -> " ... skipping the marketing part");
            }

            // try decrypting the HR part
            try {
                byte[] hrPart = decryptPart(decryptionCache, uid, bai);
                JSONObject hrJson = new JSONObject(new String(hrPart, StandardCharsets.UTF_8));
                hrJson.keys().forEachRemaining(k -> json.put(k, hrJson.getString(k)));
            } catch (AppException e) {
                // no right ignore
                logger.finer(() -> " ... skipping the HR part");
            }

            // try decrypting the security part
            try {
                byte[] securityPart = decryptPart(decryptionCache, uid, bai);
                JSONObject securityJson = new JSONObject(new String(securityPart, StandardCharsets.UTF_8));
                securityJson.keys().forEachRemaining(k -> json.put(k, securityJson.getString(k)));
            } catch (AppException e) {
                // no right ignore
                logger.finer(() -> " ... skipping the Security part");
            }

            return json.toString().getBytes(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new AppException("Unexpected exception decrypting a part: " + e.getMessage(), e);
        }
    }

    /**
     * Decrypt a part
     */
    static byte[] decryptPart(int decryptionCache, byte[] uid, ByteArrayInputStream bai)
        throws AppException, IOException {

        byte[] headerBuffer = new byte[4];
        bai.read(headerBuffer, 0, 4);
        int headerSize = ByteBuffer.wrap(headerBuffer).order(ByteOrder.BIG_ENDIAN).getInt(0);

        // Encrypted header
        byte[] encryptedHeader = new byte[headerSize];
        bai.read(encryptedHeader, 0, headerSize);
        DecryptedHeader decryptedHeader;
        try {
            decryptedHeader = Ffi.decryptHeaderUsingCache(decryptionCache, encryptedHeader);
        } catch (FfiException | CosmianException e) {
            throw new AppException("failed to decrypt the header: " + e.getMessage(), e);
        }

        byte[] blockBuffer = new byte[4];
        bai.read(blockBuffer, 0, 4);
        int blockSize = ByteBuffer.wrap(blockBuffer).order(ByteOrder.BIG_ENDIAN).getInt(0);

        byte[] encryptedContent = new byte[blockSize];
        bai.read(encryptedContent, 0, blockSize);
        try {
            return Ffi.decryptBlock(decryptedHeader.getSymmetricKey(), uid, 0, encryptedContent);
        } catch (FfiException e) {
            throw new AppException("failed to decrypt the content: " + e.getMessage(), e);
        }
    }
}
