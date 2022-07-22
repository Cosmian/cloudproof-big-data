package com.cosmian.cloudproof_demo.extractor;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.cosmian.CosmianException;
import com.cosmian.cloudproof_demo.AppException;
import com.cosmian.cloudproof_demo.Benchmarks;
import com.cosmian.cloudproof_demo.RecordUid;
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
                    String filePath = filePaths.next();
                    benchmarks.startRecording("record_total");
                    long numRecords = decryptAtPath(decryptionCache, os, inputPath.getFs(), filePath, benchmarks);
                    benchmarks.stopRecording("record_total", numRecords);
                    numEntries += numRecords;
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
     * Attempt to decrypt all records of the file at the absolute path of the given fs and write the result to os
     * 
     * @param decryptionCache
     * @param os
     * @param fs
     * @param absolutePath
     * @return the number of decrypted records
     */
    static public int decryptAtPath(int decryptionCache, OutputStream os, AppFileSystem fs, String absolutePath,
        Benchmarks benchmarks) {

        String filename = Paths.get(absolutePath).getFileName().toString();
        int mark = 0;
        int numRecords = 0;

        try (DataInputStream is = fs.getInputStream(absolutePath)) {

            while (true) {

                RecordUid uid = new RecordUid(filename, mark);
                RecordExtractor.Record record =
                    RecordExtractor.readNext(decryptionCache, uid, is, Optional.of(benchmarks));
                if (record.encryptedLength == 0) {
                    // eof
                    return numRecords;
                }
                mark += record.encryptedLength;

                // Write the result
                try {
                    os.write(record.clearText);
                    os.write('\n');
                } catch (IOException e) {
                    throw new AppException("failed writing results: " + e.getMessage(), e);
                }
                numRecords += 1;
                if (logger.isLoggable(Level.FINE)) {
                    logger.fine("successfully decrypted record #" + numRecords + " at: " + absolutePath);
                }
            }
        } catch (IOException e) {
            logger.severe(
                "decrypt: unable to read from file: " + absolutePath + ", error: " + e.getMessage() + ". skipping");
        } catch (AppException e) {
            logger.fine(() -> "Skipping process of the file: " + absolutePath + ": " + e.getMessage());
        }
        return numRecords;
    }

    static public long decryptAtPathAndPositions(int decryptionCache, OutputStream os, InputPath fsRootPath,
        String filename, List<Long> positions, Benchmarks benchmarks) {

        int numRecords = 0;
        String absolutePath = fsRootPath.resolve(filename);

        positions.sort(Comparator.naturalOrder());

        try (DataInputStream is = fsRootPath.getFs().getInputStream(absolutePath)) {

            long previousMark = 0;
            for (long mark : positions) {

                RecordUid recordUid = new RecordUid(filename, mark);

                long currentSkip = mark - previousMark;
                logger.fine(() -> "Extracting " + recordUid + ": jumping: " + currentSkip);
                long skipped = is.skip(currentSkip);
                if (skipped != currentSkip) {
                    throw new AppException("Expected to skip: " + currentSkip + " bytes, actual: " + skipped);
                }

                RecordExtractor.Record record;
                try {
                    record = RecordExtractor.readNext(decryptionCache, recordUid, is, Optional.of(benchmarks));
                } catch (AppException e) {
                    logger.fine(() -> "Skipping process of the file: " + absolutePath + ": " + e.getMessage());
                    continue;
                }
                if (record.encryptedLength == 0) {
                    // eof
                    return numRecords;
                }
                previousMark = mark + record.encryptedLength;

                // Write the result
                try {
                    os.write(record.clearText);
                    os.write('\n');
                } catch (IOException e) {
                    logger.severe("failed writing results: " + e.getMessage());
                    return numRecords;
                }
                numRecords += 1;
                if (logger.isLoggable(Level.FINE)) {
                    logger.fine("successfully decrypted record #" + numRecords + " at: " + absolutePath);
                }
            }

        } catch (IOException | AppException e) {
            logger.severe(
                "decrypt: unable to read from file: " + absolutePath + ", error: " + e.getMessage() + ". skipping");
        }
        return numRecords;
    }

    static void logBenchmarks(Benchmarks benchmarks) {
        logger.info(() -> {
            StringBuilder builder = new StringBuilder();
            long total_records = (long) benchmarks.getCount("record_process_time");
            long total_time = (long) benchmarks.getSum("total_time");

            builder.append("Decrypted ").append(String.format("%,d", total_records)).append(" records in ")
                .append(String.format("%,d", total_time / 1000)).append("ms\n");

            builder.append("  - init: ").append(String.format("%,.3f", benchmarks.getSum("init") / 1000)).append("ms (")
                .append(String.format("%,.1f", benchmarks.getSum("init") / total_time * 100.0)).append("%)\n");

            builder.append("  - hybrid decryption: ")
                .append(String.format("%,.3f", benchmarks.getSum("record_process_time") / 1000)).append("ms (")
                .append(String.format("%,.1f", benchmarks.getSum("record_process_time") / total_time * 100.0))
                .append("%)\n");

            double record_total = benchmarks.getAverage("record_total") / 1000.0;
            builder.append("Average processing time per decrypted record (excl. reading) : ")
                .append(String.format("%,.3f", record_total)).append("ms\n");

            double record_attributes_decryption = benchmarks.getAverage("record_attributes_decryption") / 1000.0;
            builder.append("  - attributes dec.: ").append(record_attributes_decryption).append("ms (")
                .append(String.format("%,.2f", record_attributes_decryption / record_total * 100.0)).append("%)\n");

            double record_bzip = benchmarks.getAverage("bzip") / 1000.0;
            builder.append("  - bzip.: ").append(record_bzip).append("ms (")
                .append(String.format("%,.2f", record_bzip / record_total * 100.0)).append("%)\n");

            double record_symmetric_decryption = benchmarks.getAverage("record_block") / 1000.0;
            builder.append("  - symmetric dec.: ").append(record_symmetric_decryption).append("ms (")
                .append(String.format("%,.2f", record_symmetric_decryption / record_total * 100.0)).append("%)\n");

            double other = record_total - record_attributes_decryption - record_bzip - record_symmetric_decryption;
            builder.append("  - other ( mostly I/O): ").append(String.format("%,.3f", other)).append("ms (")
                .append(String.format("%,.2f", other / record_total * 100.0)).append("%)\n");

            return builder.toString();
        });
    }

}
