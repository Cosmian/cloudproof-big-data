package com.cosmian.cloudproof_demo.extractor;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.logging.Logger;

import org.json.JSONObject;

import com.cosmian.CosmianException;
import com.cosmian.cloudproof_demo.AppException;
import com.cosmian.cloudproof_demo.Benchmarks;
import com.cosmian.cloudproof_demo.RecordUid;
import com.cosmian.jna.FfiException;
import com.cosmian.jna.cover_crypt.DecryptedHeader;
import com.cosmian.jna.cover_crypt.Ffi;

public class RecordExtractor {

    private static final Logger logger = Logger.getLogger(RecordExtractor.class.getName());

    public static class Record {
        public final int encryptedLength;

        public final byte[] clearText;

        public Record(int encryptedLength, byte[] clearText) {
            this.encryptedLength = encryptedLength;
            this.clearText = clearText;
        }
    }

    /**
     * Decrypt the record
     */
    public static Record readNext(int decryptionCache, RecordUid uid, DataInputStream is,
        Optional<Benchmarks> benchmarks) throws AppException {

        logger.finer(() -> "Decryption: record UID: " + uid.filename + " :: " + uid.mark);

        try {

            byte[] recordSizeBuffer = new byte[4];
            try {
                is.readFully(recordSizeBuffer);
            } catch (EOFException e) {
                // EOF
                logger.finer(() -> "EOF");
                return new Record(0, new byte[] {});
            }
            int recordSize = ByteBuffer.wrap(recordSizeBuffer).order(ByteOrder.BIG_ENDIAN).getInt(0);

            logger.finer(() -> "The next encrypted record size is: " + recordSize);

            byte[] encryptedRecord = new byte[recordSize];
            is.readFully(encryptedRecord);
            ByteArrayInputStream bai = new ByteArrayInputStream(encryptedRecord);

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

            return new Record(recordSize + 4, json.toString().getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new AppException("Unexpected exception decrypting a part: " + e.getMessage(), e);
        }
    }

    /**
     * Decrypt a part
     */
    static byte[] decryptPart(int decryptionCache, RecordUid uid, ByteArrayInputStream bai)
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
            return Ffi.decryptBlock(decryptedHeader.getSymmetricKey(), uid.toBytes(), 0, encryptedContent);
        } catch (FfiException e) {
            throw new AppException("failed to decrypt the content: " + e.getMessage(), e);
        }
    }
}
