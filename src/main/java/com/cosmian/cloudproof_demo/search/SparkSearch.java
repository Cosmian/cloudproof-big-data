package com.cosmian.cloudproof_demo.search;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;
import org.apache.spark.util.LongAccumulator;

import com.cosmian.CosmianException;
import com.cosmian.cloudproof_demo.AppException;
import com.cosmian.cloudproof_demo.Base58;
import com.cosmian.cloudproof_demo.DseDB;
import com.cosmian.cloudproof_demo.DseDB.Configuration;
import com.cosmian.cloudproof_demo.Spark;
import com.cosmian.cloudproof_demo.extractor.SparkExtractionProcess;
import com.cosmian.cloudproof_demo.sse.Sse;
import com.cosmian.cloudproof_demo.sse.Sse.DbUid;
import com.cosmian.cloudproof_demo.sse.Sse.Key;
import com.cosmian.cloudproof_demo.sse.Sse.Word;
import com.cosmian.cloudproof_demo.sse.SseFinder;
import com.cosmian.jna.FfiException;
import com.cosmian.jna.cover_crypt.Ffi;
import com.cosmian.rest.kmip.objects.PrivateKey;

public class SparkSearch implements Search {

    private static final Logger logger = Logger.getLogger(SparkSearch.class.getName());

    private final JavaSparkContext spark;

    public SparkSearch(JavaSparkContext spark) {
        this.spark = spark;
    }

    @Override
    public long[] run(Set<Word> words, boolean disjunction, String fsRootUri, Key k, String privateKeyJson,
        Configuration dseConf, String outputDirectory, String cleartextFilename) throws AppException {

        HashMap<Word, Set<DbUid>> result;
        try (DseDB dseDb = new DseDB(dseConf)) {
            try {
                result = Sse.bulkRetrieve(k, words, dseDb);
            } catch (CosmianException e) {
                throw new AppException("failed querying the SSE index: " + e.getMessage(), e);
            }
        } catch (CosmianException e) {
            throw new AppException("Failed initializing the SSE DB: " + e.getMessage(), e);
        }
        logger.fine(() -> "found results for " + result.size() + " word(s)");

        final Set<String> set = SseFinder.find(words, disjunction, k, dseConf).stream()
            .map(bytes -> Base58.encode(bytes)).collect(Collectors.toSet());

        // process the resource using a decryption cache
        // the cache should be programmatically destroyed at the end to reclaim memory
        int decryptionCache;
        try {
            PrivateKey privateKey = PrivateKey.fromJson(privateKeyJson);
            decryptionCache = Ffi.createDecryptionCache(privateKey);
        } catch (CosmianException e) {
            throw new AppException("Failed processing the private key file:" + e.getMessage(), e);
        } catch (FfiException e) {
            throw new AppException("Failed creating the decryption cache:" + e.getMessage(), e);
        }

        long allEntries = 0;

        try {
            LongAccumulator counter = spark.sc().longAccumulator();
            String path = Spark.setHadoopConf(spark, fsRootUri);
            JavaPairRDD<String, PortableDataStream> rdds = spark.binaryFiles(path + "/*").filter(tuple -> {
                URI uri;
                try {
                    uri = new URI(tuple._1());
                } catch (URISyntaxException e1) {
                    throw new AppException("invalid input file URI: " + tuple._1() + ": " + e1.getMessage(), e1);
                }
                String fileName = new File(uri.getPath()).getName();
                return set.contains(fileName);
            });
            rdds.foreachPartition(
                new SparkExtractionProcess(privateKeyJson, outputDirectory, cleartextFilename, counter));
            allEntries += counter.value();
        } finally {
            try {
                Ffi.destroyDecryptionCache(decryptionCache);
            } catch (FfiException | CosmianException e) {
                logger.warning("Failed destroying the decryption cache and reclaiming memory: " + e.getMessage());
            }
        }
        final long allEntries_ = allEntries;
        logger.info(() -> "Found " + set.size() + " records and decrypted " + allEntries_);
        return new long[] {set.size(), allEntries};
    }

}
