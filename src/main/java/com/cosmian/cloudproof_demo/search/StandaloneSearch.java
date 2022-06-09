package com.cosmian.cloudproof_demo.search;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Set;
import java.util.logging.Logger;

import com.cosmian.CosmianException;
import com.cosmian.cloudproof_demo.AppException;
import com.cosmian.cloudproof_demo.Base58;
import com.cosmian.cloudproof_demo.Benchmarks;
import com.cosmian.cloudproof_demo.DseDB;
import com.cosmian.cloudproof_demo.extractor.StandaloneExtractor;
import com.cosmian.cloudproof_demo.fs.InputPath;
import com.cosmian.cloudproof_demo.fs.OutputDirectory;
import com.cosmian.cloudproof_demo.sse.Sse.Key;
import com.cosmian.cloudproof_demo.sse.Sse.Word;
import com.cosmian.cloudproof_demo.sse.SseFinder;
import com.cosmian.jna.FfiException;
import com.cosmian.jna.cover_crypt.Ffi;
import com.cosmian.rest.kmip.objects.PrivateKey;

public class StandaloneSearch implements Search {

    private static final Logger logger = Logger.getLogger(StandaloneSearch.class.getName());

    private final Benchmarks benchmarks = new Benchmarks();

    public long[] run(Set<Word> words, boolean disjunction, String fsRootUri, Key k, String privateKeyJson,
        DseDB.Configuration dseConf, String outputDirectory, String cleartextFilename) throws AppException {

        benchmarks.startRecording("total_time");
        benchmarks.startRecording("sse");

        Set<byte[]> set = SseFinder.find(words, disjunction, k, dseConf);
        logger.fine(() -> "found " + set.size() + " UIDs matching the search");

        benchmarks.stopRecording("sse", set.size());
        benchmarks.startRecording("init");

        InputPath fsRootPath = InputPath.parse(fsRootUri);
        OutputDirectory output = OutputDirectory.parse(outputDirectory);

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

        benchmarks.stopRecording("init", 1);
        benchmarks.startRecording("record_process_time");

        long numEntries = 0;
        try {

            try (OutputStream os =
                output.getFs().getOutputStream(output.getDirectory().resolve(cleartextFilename).toString(), true)) {

                for (byte[] uid : set) {
                    benchmarks.startRecording("record_total");
                    String fileName = Base58.encode(uid);
                    logger.finer(() -> "attempting to decrypt " + fsRootPath.resolve(fileName));
                    long time = StandaloneExtractor.decryptAtPath(decryptionCache, os, fsRootPath.getFs(),
                        fsRootPath.resolve(fileName));
                    if (time > 0) {
                        benchmarks.record("decryption", 1, time);
                        numEntries += 1;
                        benchmarks.stopRecording("record_total", 1);
                    }
                }
            } catch (IOException e) {
                throw new AppException("failed opening output file: " + cleartextFilename + ": " + e.getMessage(), e);
            }
        } finally {
            try {
                Ffi.destroyDecryptionCache(decryptionCache);
            } catch (FfiException | CosmianException e) {
                logger.warning("Failed destroying the decryption cache and reclaiming memory: " + e.getMessage());
            }
            benchmarks.stopRecording("record_process_time", numEntries);
            benchmarks.stopRecording("total_time", 1);
        }
        logBenchmarks(benchmarks);
        return new long[] {set.size(), numEntries};
    }

    static void logBenchmarks(Benchmarks benchmarks) {
        logger.info(() -> {
            StringBuilder builder = new StringBuilder();
            long total_decrypted = (long) benchmarks.getCount("record_process_time");
            long total_found = (long) benchmarks.getCount("sse");
            long total_time = (long) benchmarks.getSum("total_time");

            builder.append("Found ").append(String.format("%,d", total_found)).append(" records and decrypted ")
                .append(String.format("%,d", total_decrypted)).append(" in ")
                .append(String.format("%,d", total_time / 1000)).append("ms (init: ")
                .append(String.format("%,.1f", benchmarks.getSum("init") / total_time * 100.0)).append("%, sse: ")
                .append(String.format("%,.1f", benchmarks.getSum("sse") / total_time * 100.0))
                .append("%, hybrid decryption: ")
                .append(String.format("%,.1f", benchmarks.getSum("record_process_time") / total_time * 100.0))
                .append("%)\n");

            double record_total = benchmarks.getAverage("record_total") / 1000.0;
            builder.append("Average processing time per decrypted record : ")
                .append(String.format("%,.3f", record_total)).append("ms");

            double record_decryption = benchmarks.getAverage("decryption") / 1000.0;
            builder.append(", hybrid decryption: ").append(record_decryption).append("ms (")
                .append(String.format("%,.2f", record_decryption / record_total * 100.0)).append("%)\n");

            return builder.toString();
        });
    }

}
