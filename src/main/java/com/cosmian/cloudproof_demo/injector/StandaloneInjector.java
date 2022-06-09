package com.cosmian.cloudproof_demo.injector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;

import com.cosmian.CosmianException;
import com.cosmian.cloudproof_demo.AppException;
import com.cosmian.cloudproof_demo.Benchmarks;
import com.cosmian.cloudproof_demo.DseDB;
import com.cosmian.cloudproof_demo.fs.InputPath;
import com.cosmian.cloudproof_demo.fs.OutputDirectory;
import com.cosmian.cloudproof_demo.sse.Sse.Key;
import com.cosmian.cloudproof_demo.sse.SseUpserter;
import com.cosmian.jna.FfiException;
import com.cosmian.jna.cover_crypt.Ffi;
import com.cosmian.rest.kmip.objects.PublicKey;

public class StandaloneInjector implements Injector {

    private static final Logger logger = Logger.getLogger(StandaloneInjector.class.getName());

    private final Benchmarks benchmarks = new Benchmarks();

    public StandaloneInjector() {

    }

    @Override
    public void run(Key k, Key kStar, String publicKeyJson, String outputDirectory, DseDB.Configuration dseConf,
        List<String> inputs) throws AppException {

        benchmarks.startRecording("total_time");
        benchmarks.startRecording("init");

        // The sha256 is used to calculate hashes
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("sha-256");
        } catch (NoSuchAlgorithmException e1) {
            throw new AppException("Sha 256 is not supported on this machine; cannot continue");
        }

        // Pre-process the access to the output directory
        OutputDirectory output = OutputDirectory.parse(outputDirectory);

        // cache the encryption key for efficiency
        int cacheHandle;
        try {
            PublicKey publicKey = PublicKey.fromJson(publicKeyJson);
            cacheHandle = Ffi.createEncryptionCache(publicKey);
        } catch (CosmianException e) {
            throw new AppException("Failed processing the public key file:" + e.getMessage(), e);
        } catch (FfiException e) {
            throw new AppException("Failed creating the cache:" + e.getMessage(), e);
        }

        // truncate the DSE Index Tables - you may want to remove this
        SseUpserter.truncate(dseConf);

        // done with init - now start processing records
        benchmarks.stopRecording("init", 1);
        benchmarks.startRecording("record_process_time");

        Optional<Benchmarks> doBench = Optional.of(benchmarks);
        long numRecords = 0;
        try (SseUpserter sseUpserter = new SseUpserter(k, kStar, dseConf, Optional.of(benchmarks))) {

            for (String inputPathString : inputs) {
                InputPath inputPath = InputPath.parse(inputPathString);
                Iterator<String> it = inputPath.listFiles();
                while (it.hasNext()) {
                    String inputFile = it.next();
                    try {

                        InputStream is = inputPath.getFs().getInputStream(inputFile);

                        try (BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
                            String line;
                            while ((line = br.readLine()) != null) {
                                if (line.trim().length() == 0) {
                                    continue;
                                }
                                numRecords += 1;
                                RecordInjector.process(line, md, sseUpserter, cacheHandle, doBench, output);
                            }
                        } catch (IOException e) {
                            throw new AppException("an /IO Error occurred:" + e.getMessage(), e);
                        }

                    } catch (AppException e) {
                        logger.severe("Aborting processing of the file: " + inputFile + ": " + e.getMessage());
                    }
                }
            }
        } finally {
            // The cache should be destroyed to reclaim memory
            benchmarks.stopRecording("record_process_time", numRecords);
            benchmarks.stopRecording("total_time", 1);
            logBenchmarks(benchmarks);
            try {
                Ffi.destroyEncryptionCache(cacheHandle);
            } catch (FfiException | CosmianException e) {
                logger.warning("Failed destroying the encryption cache and reclaiming memory: " + e.getMessage());
            }
        }

    }

    static void logBenchmarks(Benchmarks benchmarks) {
        logger.info(() -> {
            StringBuilder builder = new StringBuilder();
            long total_records = (long) benchmarks.getCount("record_process_time");
            long total_time = (long) benchmarks.getSum("total_time");

            builder.append("Processed ").append(String.format("%,d", total_records)).append(" records in ")
                .append(String.format("%,d", total_time / 1000)).append("ms (init: ")
                .append(String.format("%,.1f", benchmarks.getSum("init") / total_time * 100.0))
                .append("%, processing: ")
                .append(String.format("%,.1f", benchmarks.getSum("record_process_time") / total_time * 100.0))
                .append("%)\n");

            double record_total = benchmarks.getAverage("record_total") / 1000.0;
            builder.append("Average processing time per record (after reading): ")
                .append(String.format("%,.3f", record_total)).append("ms\n");

            double record_preprocessing = benchmarks.getAverage("record_pre_processing") / 1000.0;
            builder.append("  - parsing: ").append(record_preprocessing).append("ms (")
                .append(String.format("%,.2f", record_preprocessing / record_total * 100.0)).append("%)\n");

            double record_sse_crypto = benchmarks.getAverage("record_sse_crypto") / 1000.0;
            builder.append("  - SSE crypto: ").append(record_sse_crypto).append("ms (")
                .append(String.format("%,.2f", record_sse_crypto / record_total * 100.0)).append("%)\n");

            double record_sse_db = benchmarks.getAverage("record_sse_db") / 1000.0;
            builder.append("  - SSE DB: ").append(record_sse_db).append("ms (")
                .append(String.format("%,.2f", record_sse_db / record_total * 100.0)).append("%)\n");

            double record_attributes_encryption = benchmarks.getAverage("record_attributes_encryption") / 1000.0;
            builder.append("  - attributes enc.: ").append(record_attributes_encryption).append("ms (")
                .append(String.format("%,.2f", record_attributes_encryption / record_total * 100.0)).append("%)\n");

            double record_bzip = benchmarks.getAverage("bzip") / 1000.0;
            builder.append("  - bzip.: ").append(record_bzip).append("ms (")
                .append(String.format("%,.2f", record_bzip / record_total * 100.0)).append("%)\n");

            double record_symmetric_encryption = benchmarks.getAverage("record_block") / 1000.0;
            builder.append("  - symmetric enc.: ").append(record_symmetric_encryption).append("ms (")
                .append(String.format("%,.2f", record_symmetric_encryption / record_total * 100.0)).append("%)\n");

            double record_write = benchmarks.getAverage("record_write") / 1000.0;
            builder.append("  - write (I/O): ").append(record_write).append("ms (")
                .append(String.format("%,.2f", record_write / record_total * 100.0)).append("%)\n");

            return builder.toString();
        });
    }

}
