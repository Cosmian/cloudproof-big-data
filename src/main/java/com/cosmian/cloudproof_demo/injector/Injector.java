package com.cosmian.cloudproof_demo.injector;

import java.util.List;

import com.cosmian.cloudproof_demo.AppException;
import com.cosmian.cloudproof_demo.DseDB;
import com.cosmian.cloudproof_demo.sse.Sse.Key;

public interface Injector {

    /**
     * Inject data for encryption and secure indexing
     * 
     * @param k the Findex first symmetric key, shared between updaters and readers
     * @param kStar the second Findex symmetric key only available to updaters
     * @param publicKeyJson the attributes Encryption (CoverCrypt or GPSW) public key in KMIP 2.1 JSON format
     * @param outputDirectory the directory URL where to output encrypted files
     * @param dseConf the Cassandra DSE configuration
     * @param inputs the input filenames ot Kafka topics
     * @param kafkaTopics true when the inputs are Kafka topics
     * @param maxSizeInMB the maximum size fo an encrypted file in mega bytes before it rolls over
     * @param maxAgeInSeconds the maximum age in seconds of an encrypted file before it rolls overs
     * @param dropIndexes true when Findex indexes should be first dropped before encrypting and indexing
     * @throws AppException
     */
    void run(Key k, Key kStar, String publicKeyJson, String outputDirectory, DseDB.Configuration dseConf,
        List<String> inputs, boolean kafkaTopics, int maxSizeInMB, int maxAgeInSeconds, boolean dropIndexes)
        throws AppException;
}
