package com.cosmian.cloudproof_demo.extractor;

import java.util.List;
import java.util.logging.Logger;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;
import org.apache.spark.util.LongAccumulator;

import com.cosmian.CosmianException;
import com.cosmian.cloudproof_demo.AppException;
import com.cosmian.cloudproof_demo.Spark;
import com.cosmian.jna.FfiException;
import com.cosmian.jna.cover_crypt.Ffi;
import com.cosmian.rest.kmip.objects.PrivateKey;

public class SparkExtractor implements Extractor {

    private static final Logger logger = Logger.getLogger(SparkExtractor.class.getName());

    private final JavaSparkContext spark;

    public SparkExtractor(JavaSparkContext spark) {
        this.spark = spark;
    }

    @Override
    public long run(List<String> inputs, String privateKeyJson, String outputDirectory, String clearTextFilename)
        throws AppException {

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

            for (String inputPathUri : inputs) {
                LongAccumulator counter = spark.sc().longAccumulator();
                String path = Spark.setHadoopConf(spark, inputPathUri);
                JavaPairRDD<String, PortableDataStream> rdds = spark.binaryFiles(path + "/*");
                rdds.foreachPartition(
                    new SparkExtractAllProcess(privateKeyJson, outputDirectory, clearTextFilename, counter));
                allEntries += counter.value();
            }
        } finally {
            try {
                Ffi.destroyDecryptionCache(decryptionCache);
            } catch (FfiException | CosmianException e) {
                logger.warning("Failed destroying the decryption cache and reclaiming memory: " + e.getMessage());
            }
        }
        return allEntries;
    }

}
