package com.cosmian.cloudproof_demo.injector;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.json.JSONObject;

import com.cosmian.CosmianException;
import com.cosmian.cloudproof_demo.AppException;
import com.cosmian.cloudproof_demo.Benchmarks;
import com.cosmian.cloudproof_demo.RecordUid;
import com.cosmian.cloudproof_demo.policy.Country;
import com.cosmian.cloudproof_demo.policy.Department;
import com.cosmian.cloudproof_demo.sse.Sse.Word;
import com.cosmian.cloudproof_demo.sse.SseUpserter;
import com.cosmian.jna.FfiException;
import com.cosmian.jna.cover_crypt.EncryptedHeader;
import com.cosmian.jna.cover_crypt.Ffi;
import com.cosmian.rest.cover_crypt.acccess_policy.Attr;

/**
 * This class exposes the main method that processes a record, whether in Spark or in the standalone runner
 */
public class RecordInjector {

    private static final Logger logger = Logger.getLogger(StandaloneInjector.class.getName());

    /**
     * Process the record, returning its uid
     */
    public static RecordUid process(String line, MessageDigest sha256, SseUpserter sseUpserter, int encryptionCache,
        Optional<Benchmarks> benchmarks, OutputFile outputFile) throws AppException {

        if (benchmarks.isPresent()) {
            benchmarks.get().startRecording("record_total");
            benchmarks.get().startRecording("record_pre_processing");
        }

        RecordUid recordUid = outputFile.nextRecordUid();
        logger.finer(() -> "Encryption: record UID: " + recordUid.filename + " :: " + recordUid.mark);
        byte[] uid = recordUid.toBytes();

        // recover data for indexing and attributes encryption
        JSONObject json = new JSONObject(line);

        String country = json.getString("country");
        String firstName = json.getString("firstName").toLowerCase();
        String lastName = json.getString("lastName").toLowerCase();

        String[] indexedValues =
            new String[] {firstName, lastName, "first=" + firstName, "last=" + lastName, "country=" + country};
        Set<Word> set = Arrays.stream(indexedValues)
            .map(v -> new Word(v.toLowerCase().getBytes(StandardCharsets.UTF_8))).collect(Collectors.toSet());
        if (benchmarks.isPresent())
            benchmarks.get().stopRecording("record_pre_processing", 1);

        // Indexing
        sseUpserter.upsert(uid, set);

        if (benchmarks.isPresent())
            benchmarks.get().startRecording("record_attributes_encryption");

        // Attributes Encryption- the outputStream
        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        Attr country_attribute;
        try {

            // get country attribute
            country_attribute = Country.from(country).getAttribute();

            // attributes for common part of the file
            Attr[] common_attributes =
                new Attr[] {country_attribute, Department.Marketing.getAttribute(), Department.HR.getAttribute()};
            JSONObject common_json = new JSONObject();
            common_json.put("firstName", json.getString("firstName"));
            common_json.put("lastName", json.getString("lastName"));
            common_json.put("country", country);
            byte[] commonBytes = common_json.toString().getBytes(StandardCharsets.UTF_8);
            encryptPart(bao, encryptionCache, common_attributes, uid, commonBytes);

            // marketing part of the file
            Attr[] mkg_attributes = new Attr[] {country_attribute, Department.Marketing.getAttribute()};
            // we want all but employeeNumber and security
            JSONObject mkg_json = new JSONObject();
            mkg_json.put("region", json.getString("region"));
            encryptPart(bao, encryptionCache, mkg_attributes, uid,
                mkg_json.toString().getBytes(StandardCharsets.UTF_8));

            // HR part of the file
            Attr[] hr_attributes = new Attr[] {country_attribute, Department.HR.getAttribute()};
            JSONObject hr_json = new JSONObject();
            hr_json.put("phone", json.getString("phone"));
            hr_json.put("email", json.getString("email"));
            hr_json.put("employeeNumber", json.getString("employeeNumber"));
            encryptPart(bao, encryptionCache, hr_attributes, uid, hr_json.toString().getBytes(StandardCharsets.UTF_8));

            // security part of the file
            Attr[] security_attributes = new Attr[] {country_attribute, Department.Security.getAttribute()}; // marketing
            JSONObject security_json = new JSONObject();
            security_json.put("security", json.getString("security"));
            encryptPart(bao, encryptionCache, security_attributes, uid,
                security_json.toString().getBytes(StandardCharsets.UTF_8));

        } catch (CosmianException e) {
            throw new AppException("Invalid attributes:" + e.getMessage(), e);
        }

        if (benchmarks.isPresent()) {
            benchmarks.get().stopRecording("record_attributes_encryption", 1);
            benchmarks.get().startRecording("record_write");
        }

        // write the result
        outputFile.write(bao.toByteArray());

        if (benchmarks.isPresent()) {
            benchmarks.get().stopRecording("record_write", 1);
            benchmarks.get().stopRecording("record_total", 1);
        }

        logger.fine(() -> "Injected:" + recordUid + " with country: " + country_attribute.toString()
            + ", indexed values: " + Arrays.toString(indexedValues));

        return recordUid;
    }

    static void encryptPart(ByteArrayOutputStream bao, int encryptionCache, Attr[] attributes, byte[] hash,
        byte[] clearText) throws AppException {
        EncryptedHeader encryptedHeader;
        try {
            encryptedHeader = Ffi.encryptHeaderUsingCache(encryptionCache, attributes);
        } catch (FfiException | CosmianException e) {
            throw new AppException("Failed to encrypt the header: " + e.getMessage(), e);
        }
        byte[] encryptedBlock;
        try {
            encryptedBlock = Ffi.encryptBlock(encryptedHeader.getSymmetricKey(), hash, 0, clearText);
        } catch (FfiException e) {
            throw new AppException("Failed to encrypt the content: " + e.getMessage(), e);
        }

        // The size of the header as an int in BE bytes
        int headerLength = encryptedHeader.getEncryptedHeaderBytes().length;
        ByteBuffer headerSize = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(headerLength);
        // The size of the header as an int in BE bytes
        ByteBuffer blockSize = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(encryptedBlock.length);

        // Write the result
        try {
            bao.write(headerSize.array());
            bao.write(encryptedHeader.getEncryptedHeaderBytes());
            bao.write(blockSize.array());
            bao.write(encryptedBlock);
            bao.flush();
        } catch (IOException e) {
            throw new AppException("Failed to write the encrypted bytes: " + e.getMessage(), e);
        }

        logger.finer(() -> "Encrypted part with attributes: " + Arrays.toString(attributes) + ", header length: "
            + headerLength);
    }

}
