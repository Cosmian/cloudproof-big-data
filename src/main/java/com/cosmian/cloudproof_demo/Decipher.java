package com.cosmian.cloudproof_demo;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.cosmian.CosmianException;
import com.cosmian.cloudproof_demo.fs.AppFileSystem;
import com.cosmian.cloudproof_demo.fs.InputPath;
import com.cosmian.cloudproof_demo.fs.OutputDirectory;
import com.cosmian.jna.Ffi;
import com.cosmian.jna.FfiException;
import com.cosmian.jna.abe.DecryptedHeader;
import com.cosmian.rest.kmip.objects.PrivateKey;

import org.json.JSONObject;

public class Decipher implements AutoCloseable {

    private static final Logger logger = Logger.getLogger(Decipher.class.getName());

    final Path privateKeyFile;

    final int decryptionCache;

    public Decipher(Path privateKeyFile) throws AppException {
        this.privateKeyFile = privateKeyFile;

        // process the resource using a decryption cache
        // the cache should be programmatically destroyed at the end to reclaim memory
        int cache;
        try {
            String privateKeyJson = LocalResource.load_file_string(this.privateKeyFile.toFile());
            PrivateKey privateKey = PrivateKey.fromJson(privateKeyJson);
            cache = Ffi.createDecryptionCache(privateKey);
        } catch (CosmianException e) {
            throw new AppException("Failed processing the private key file:" + e.getMessage(), e);
        } catch (IOException e) {
            throw new AppException("Failed loading the private key file:" + e.getMessage(), e);
        } catch (FfiException e) {
            throw new AppException("Failed creating the decryption cache:" + e.getMessage(), e);
        }
        this.decryptionCache = cache;
    }

    public void run(List<String> inputs, String outputDirectory, String outputFilename) throws AppException {

        OutputDirectory output = OutputDirectory.parse(outputDirectory);
        try (OutputStream os = output.getFs().getOutputStream(output.getDirectory().resolve(outputFilename).toString(),
                true)) {

            for (String inputPathString : inputs) {

                InputPath inputPath = InputPath.parse(inputPathString);

                final long then = System.nanoTime();
                long decryptionTime = 0;
                long numEntries = 0;
                Iterator<String> filePaths = inputPath.listFiles();
                while (filePaths.hasNext()) {
                    String filePath = filePaths.next();
                    long time = decryptAtPath(os, inputPath.getFs(), filePath);
                    if (time > 0) {
                        decryptionTime += time;
                        numEntries += 1;
                    }
                }
                // logging
                final long totalTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - then);
                final long totalEntries = numEntries;
                final long totalDecryptionTime = decryptionTime;
                if (totalEntries > 0) {
                    logger.info(() -> "Processed " + totalEntries + " lines to " + outputFilename + " in " + totalTime
                            + " ms (decryption time: " + (totalDecryptionTime * 100 / totalTime)
                            + "%). Average decryption time per line: " + totalDecryptionTime / totalEntries + " ms.");
                } else {
                    logger.info("No line was decrypted");
                }
            }
        } catch (IOException e) {
            throw new AppException("failed opening output file: " + outputFilename + ": " + e.getMessage(), e);
        }
    }

    /**
     * Attempt to decrypt the file at the absolute path of the given fs and write
     * the result to os
     * 
     * @param decryptionCache
     * @param os
     * @param fs
     * @param absolutePath
     * @return 0 if the decryption failed, the decryption time in milliseconds
     *         otherwise
     */
    long decryptAtPath(OutputStream os, AppFileSystem fs, String absolutePath) {

        long decryptionTime = 0;
        byte[] uid = Base58.decode(Paths.get(absolutePath).getFileName().toString());

        try {
            byte[] encryptedBytes = fs.readFile(absolutePath);
            // Measure decryption time (quick and dirt - need a micro benchmark tool to do
            // this properly)
            final long encryptionThen = System.nanoTime();
            byte[] clearText = decryptResource(uid, encryptedBytes);
            decryptionTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - encryptionThen);
            // Write the result
            try {
                os.write(clearText);
                os.write('\n');
            } catch (IOException e) {
                throw new AppException("failed writing results: " + e.getMessage(), e);
            }
            if (logger.isLoggable(Level.FINE)) {
                logger.finer(() -> "Finished decrypting: " + absolutePath);
            }
        } catch (AppException e) {
            logger.fine(() -> "Skipping process of the file: " + absolutePath + ": " + e.getMessage());
            decryptionTime = 0;
        }
        return decryptionTime;
    }

    /**
     * Process the resource, returning the average decryption time in millis
     */
    byte[] decryptResource(byte[] uid, byte[] encryptedBytes)
            throws AppException {

        ByteArrayInputStream bai = new ByteArrayInputStream(encryptedBytes);

        try {
            // decrypt the common part
            byte[] commonPart = decryptPart(uid, bai);
            JSONObject json = new JSONObject(new String(commonPart, StandardCharsets.UTF_8));

            // try decrypting the marketing part
            try {
                byte[] mkgPart = decryptPart(uid, bai);
                JSONObject mkgJson = new JSONObject(new String(mkgPart, StandardCharsets.UTF_8));
                mkgJson.keys().forEachRemaining(k -> json.put(k, mkgJson.getString(k)));
            } catch (AppException e) {
                // no right ignore
                logger.finer(() -> " ... skipping the marketing part");
            }

            // try decrypting the HR part
            try {
                byte[] hrPart = decryptPart(uid, bai);
                JSONObject hrJson = new JSONObject(new String(hrPart, StandardCharsets.UTF_8));
                hrJson.keys().forEachRemaining(k -> json.put(k, hrJson.getString(k)));
            } catch (AppException e) {
                // no right ignore
                logger.finer(() -> " ... skipping the HR part");
            }

            // try decrypting the security part
            try {
                byte[] securityPart = decryptPart(uid, bai);
                JSONObject securityJson = new JSONObject(new String(securityPart, StandardCharsets.UTF_8));
                securityJson.keys().forEachRemaining(k -> json.put(k, securityJson.getString(k)));
            } catch (AppException e) {
                // no right ignore
                logger.finer(() -> " ... skipping the Security part");
            }

            return json.toString().getBytes(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new AppException("Unexpected exception decrypting a part: " + e.getMessage(), e);
        }

    }

    /**
     * Decrypt a part
     */
    byte[] decryptPart(byte[] uid, ByteArrayInputStream bai)
            throws AppException, IOException {

        byte[] headerBuffer = new byte[4];
        bai.read(headerBuffer, 0, 4);
        int headerSize = ByteBuffer.wrap(headerBuffer).order(ByteOrder.BIG_ENDIAN).getInt(0);

        // Encrypted header
        byte[] encryptedHeader = new byte[headerSize];
        bai.read(encryptedHeader, 0, headerSize);
        DecryptedHeader decryptedHeader;
        try {
            decryptedHeader = Ffi.decryptHeaderUsingCache(this.decryptionCache, encryptedHeader);
        } catch (FfiException | CosmianException e) {
            throw new AppException("failed to decrypt the header: " + e.getMessage(), e);
        }

        byte[] blockBuffer = new byte[4];
        bai.read(blockBuffer, 0, 4);
        int blockSize = ByteBuffer.wrap(blockBuffer).order(ByteOrder.BIG_ENDIAN).getInt(0);

        byte[] encryptedContent = new byte[blockSize];
        bai.read(encryptedContent, 0, blockSize);
        try {
            return Ffi.decryptBlock(decryptedHeader.getSymmetricKey(), uid, 0, encryptedContent);
        } catch (FfiException e) {
            throw new AppException("failed to decrypt the content: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        try {
            Ffi.destroyDecryptionCache(this.decryptionCache);
        } catch (FfiException | CosmianException e) {
            logger.warning("Failed destroying the decryption cache and reclaiming memory: " + e.getMessage());
        }
    }

}
