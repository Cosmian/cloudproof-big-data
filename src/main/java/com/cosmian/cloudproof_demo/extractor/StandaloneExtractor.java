package com.cosmian.cloudproof_demo.extractor;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.cosmian.CosmianException;
import com.cosmian.cloudproof_demo.AppException;
import com.cosmian.cloudproof_demo.Base58;
import com.cosmian.cloudproof_demo.Benchmarks;
import com.cosmian.cloudproof_demo.fs.AppFileSystem;
import com.cosmian.cloudproof_demo.fs.InputPath;
import com.cosmian.cloudproof_demo.fs.OutputDirectory;
import com.cosmian.jna.FfiException;
import com.cosmian.jna.cover_crypt.Ffi;
import com.cosmian.rest.kmip.objects.PrivateKey;

public class StandaloneExtractor implements Extractor {

    private static final Logger logger = Logger.getLogger(StandaloneExtractor.class.getName());

    private final Benchmarks benchmarks = new Benchmarks();

    public StandaloneExtractor() {
    }

    @Override
    public long run(List<String> inputs, String privateKeyJson, String outputDirectory, String clearTextFilename)
        throws AppException {

        benchmarks.startRecording("total_time");
        benchmarks.startRecording("init");

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
        OutputDirectory output = OutputDirectory.parse(outputDirectory);

        benchmarks.stopRecording("init", 1);
        benchmarks.startRecording("record_process_time");

        try (OutputStream os =
            output.getFs().getOutputStream(output.getDirectory().resolve(clearTextFilename).toString(), true)) {

            for (String inputPathString : inputs) {

                InputPath inputPath = InputPath.parse(inputPathString);
                long numEntries = 0;
                Iterator<String> filePaths = inputPath.listFiles();

                while (filePaths.hasNext()) {
                    benchmarks.startRecording("record_total");
                    String filePath = filePaths.next();
                    long time = decryptAtPath(decryptionCache, os, inputPath.getFs(), filePath);
                    if (time > 0) {
                        benchmarks.record("decryption", 1, time);
                        numEntries += 1;
                        benchmarks.stopRecording("record_total", 1);
                    }
                }

                allEntries += numEntries;
            }
        } catch (IOException e) {
            throw new AppException("failed opening output file: " + clearTextFilename + ": " + e.getMessage(), e);
        } finally {
            try {
                Ffi.destroyDecryptionCache(decryptionCache);
            } catch (FfiException | CosmianException e) {
                logger.warning("Failed destroying the decryption cache and reclaiming memory: " + e.getMessage());
            }
            benchmarks.stopRecording("record_process_time", allEntries);
            benchmarks.stopRecording("total_time", 1);
        }
        logBenchmarks(benchmarks);
        return allEntries;
    }

    /**
     * Attempt to decrypt the file at the absolute path of the given fs and write the result to os
     * 
     * @param decryptionCache
     * @param os
     * @param fs
     * @param absolutePath
     * @return 0 if the decryption failed, the decryption time in milliseconds otherwise
     */
    static public long decryptAtPath(int decryptionCache, OutputStream os, AppFileSystem fs, String absolutePath) {

        long decryptionTime = 0;
        byte[] uid = Base58.decode(Paths.get(absolutePath).getFileName().toString());

        try {
            byte[] encryptedBytes = fs.readFile(absolutePath);
            // Measure decryption time (quick and dirt - need a micro benchmark tool to do
            // this properly)
            final long encryptionThen = System.nanoTime();
            byte[] clearText = RecordExtractor.process(decryptionCache, uid, encryptedBytes);
            decryptionTime = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - encryptionThen);
            // Write the result
            try {
                os.write(clearText);
                os.write('\n');
            } catch (IOException e) {
                throw new AppException("failed writing results: " + e.getMessage(), e);
            }
            if (logger.isLoggable(Level.FINE)) {
                logger.finer(() -> "successfully decrypted: " + absolutePath);
            }
        } catch (AppException e) {
            logger.fine(() -> "Skipping process of the file: " + absolutePath + ": " + e.getMessage());
            decryptionTime = 0;
        }
        return decryptionTime;
    }

    static void logBenchmarks(Benchmarks benchmarks) {
        logger.info(() -> {
            StringBuilder builder = new StringBuilder();
            long total_records = (long) benchmarks.getCount("record_process_time");
            long total_time = (long) benchmarks.getSum("total_time");

            builder.append("Decrypted ").append(String.format("%,d", total_records)).append(" records in ")
                .append(String.format("%,d", total_time / 1000)).append("ms (init: ")
                .append(String.format("%,.1f", benchmarks.getSum("init") / total_time * 100.0))
                .append("%, processing: ")
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
