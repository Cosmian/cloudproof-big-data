package com.cosmian.cloudproof_demo.search;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;

import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.input.PortableDataStream;
import org.apache.spark.util.LongAccumulator;

import com.cosmian.CosmianException;
import com.cosmian.cloudproof_demo.AppException;
import com.cosmian.cloudproof_demo.RecordUid;
import com.cosmian.cloudproof_demo.extractor.RecordExtractor;
import com.cosmian.cloudproof_demo.fs.OutputDirectory;
import com.cosmian.jna.FfiException;
import com.cosmian.jna.cover_crypt.Ffi;
import com.cosmian.rest.kmip.objects.PrivateKey;

import scala.Tuple2;

public class SparkSelectedExtractionProcess implements VoidFunction<Iterator<Tuple2<String, PortableDataStream>>> {

    private static final Logger logger = Logger.getLogger(SparkSelectedExtractionProcess.class.getName());

    private final String privateKeyJson;

    private final LongAccumulator counter;

    private final String outputDirectory;

    private final String clearTextFilename;

    private final HashMap<String, List<Long>> positionsPerFile;

    public SparkSelectedExtractionProcess(String privateKeyJson, String outputDirectory, String clearTextFilename,
        LongAccumulator counter, HashMap<String, List<Long>> positionsPerFile) {
        this.privateKeyJson = privateKeyJson;
        this.counter = counter;
        this.outputDirectory = outputDirectory;
        this.clearTextFilename = clearTextFilename;
        this.positionsPerFile = positionsPerFile;
    }

    @Override
    public void call(Iterator<Tuple2<String, PortableDataStream>> iter) throws Exception {

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

        OutputDirectory output = OutputDirectory.parse(outputDirectory);
        String outputFilePath = output.getDirectory().resolve(clearTextFilename).toString();

        try (OutputStream os = output.getFs().getOutputStream(outputFilePath, true)) {

            while (iter.hasNext()) {
                Tuple2<String, PortableDataStream> tuple = iter.next();

                URI uri;
                try {
                    uri = new URI(tuple._1());
                } catch (URISyntaxException e1) {
                    throw new AppException("invalid input file URI: " + tuple._1() + ": " + e1.getMessage(), e1);
                }

                final String filename = new File(uri.getPath()).getName();

                // recover the sorted positions to extract from the given file
                List<Long> positions = this.positionsPerFile.get(filename);
                positions.sort(Comparator.naturalOrder());
                logger.info(() -> "file: " + filename + " has " + positions.size() + " entries");

                try (DataInputStream is = tuple._2().open()) {

                    long previousMark = 0;
                    for (final long mark : positions) {

                        RecordUid uid = new RecordUid(filename, mark);

                        // skip to the next mark
                        long currentSkip = mark - previousMark;
                        logger.fine(() -> "Extracting " + uid + ": jumping: " + currentSkip);
                        long skipped = is.skip(currentSkip);
                        if (skipped != currentSkip) {
                            throw new AppException("Expected to skip: " + currentSkip + " bytes, actual: " + skipped);
                        }

                        RecordExtractor.Record record;
                        try {
                            record = RecordExtractor.readNext(decryptionCache, uid, is, Optional.empty());
                        } catch (AppException e) {
                            logger.fine(() -> "Skipping process of the file: " + filename + ": " + e.getMessage());
                            continue;
                        }
                        if (record.encryptedLength == 0) {
                            // EOF
                            break;
                        }
                        previousMark = mark + record.encryptedLength;
                        try {
                            os.write(record.clearText);
                            os.write('\n');
                        } catch (IOException e) {
                            throw new AppException("failed writing results: " + e.getMessage(), e);
                        }
                        counter.add(1);
                    }
                } catch (IOException e) {
                    logger.fine(() -> "Skipping process of the file: " + filename + ": " + e.getMessage());
                    continue;
                }
            }
        } catch (IOException e) {
            throw new AppException("failed opening output file: " + clearTextFilename + ": " + e.getMessage(), e);

        } finally {
            try {
                Ffi.destroyDecryptionCache(decryptionCache);
            } catch (FfiException | CosmianException e) {
                logger.warning("Failed destroying the decryption cache and reclaiming memory: " + e.getMessage());
            }
        }

    }

}
