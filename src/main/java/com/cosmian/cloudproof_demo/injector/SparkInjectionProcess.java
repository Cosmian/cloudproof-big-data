package com.cosmian.cloudproof_demo.injector;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.Optional;
import java.util.logging.Logger;

import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.LongAccumulator;

import com.cosmian.CosmianException;
import com.cosmian.cloudproof_demo.AppException;
import com.cosmian.cloudproof_demo.DseDB;
import com.cosmian.cloudproof_demo.fs.OutputDirectory;
import com.cosmian.cloudproof_demo.sse.Sse.Key;
import com.cosmian.cloudproof_demo.sse.SseUpserter;
import com.cosmian.jna.FfiException;
import com.cosmian.jna.cover_crypt.Ffi;
import com.cosmian.rest.kmip.objects.PublicKey;

class SparkInjectionProcess implements VoidFunction<Iterator<String>> {

    private static final Logger logger = Logger.getLogger(SparkInjectionProcess.class.getName());

    private final Key k;

    private final Key kStar;

    private final String publicKeyJson;

    private final DseDB.Configuration dseConf;

    private final String outputDirectory;

    private final LongAccumulator counter;

    public SparkInjectionProcess(Key k, Key kStar, String publicKeyJson, DseDB.Configuration dseConf,
        String outputDirectory, LongAccumulator counter) {
        this.k = k;
        this.kStar = kStar;
        this.dseConf = dseConf;
        this.outputDirectory = outputDirectory;
        this.publicKeyJson = publicKeyJson;
        this.counter = counter;
    }

    public void call(Iterator<String> iter) throws AppException {

        MessageDigest sha256;
        try {
            sha256 = MessageDigest.getInstance("sha-256");
        } catch (NoSuchAlgorithmException e1) {
            throw new AppException("Sha 256 is not supported on this machine; cannot continue");
        }

        OutputDirectory output = OutputDirectory.parse(outputDirectory);

        int encryptionCacheHandle;
        try {
            PublicKey publicKey = PublicKey.fromJson(publicKeyJson);
            encryptionCacheHandle = Ffi.createEncryptionCache(publicKey);
        } catch (CosmianException e) {
            throw new AppException("Failed processing the public key file:" + e.getMessage(), e);
        } catch (FfiException e) {
            throw new AppException("Failed creating the encryption cache:" + e.getMessage(), e);
        }

        try (SseUpserter sseUpserter = new SseUpserter(k, kStar, dseConf, Optional.empty())) {
            while (iter.hasNext()) {
                byte[] uid = RecordInjector.process(iter.next(), sha256, sseUpserter, encryptionCacheHandle,
                    Optional.empty(), output);
                if (uid != null) {
                    counter.add(1);
                }
            }
        } finally {
            try {
                Ffi.destroyEncryptionCache(encryptionCacheHandle);
            } catch (FfiException | CosmianException e) {
                logger.warning(() -> "Failed destroying the encryption cache:" + e.getMessage());
            }
        }

    }
}
