package com.cosmian.cloudproof_demo;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.cosmian.CosmianException;
import com.cosmian.cloudproof_demo.fs.InputPath;
import com.cosmian.cloudproof_demo.fs.LocalFileSystem;
import com.cosmian.cloudproof_demo.fs.OutputDirectory;
import com.cosmian.cloudproof_demo.sse.Sse;
import com.cosmian.cloudproof_demo.sse.Sse.DbUid;
import com.cosmian.cloudproof_demo.sse.Sse.Key;
import com.cosmian.cloudproof_demo.sse.Sse.Word;

public class Search implements AutoCloseable {

    private static final Logger logger = Logger.getLogger(Cipher.class.getName());

    // final Path privateKeyFile;
    final Decipher decipher;

    final Key k;

    final DseDB sseDb;

    final InputPath fsRootPath;

    public Search(Path privateKeyFile, DseDB.Configuration dseConf, String fsRootUri) throws AppException {
        this.decipher = new Decipher(privateKeyFile);
        this.fsRootPath = InputPath.parse(fsRootUri);

        Path keysDirectory = privateKeyFile.getParent();
        File kFile = keysDirectory.resolve(KeyGenerator.SSE_K_KEY_FILENAME).toFile();
        this.k = new Key(new LocalFileSystem().readFile(kFile));

        try {
            this.sseDb = new DseDB(dseConf);
        } catch (CosmianException e) {
            throw new AppException("Failed initializing the SSE DB: " + e.getMessage(), e);
        }

    }

    public void run(Set<Word> words, String outputDirectory, String outputFilename) throws AppException {

        final long then = System.nanoTime();

        HashMap<Word, Set<DbUid>> result;
        try {
            result = Sse.bulkRetrieve(this.k, words, this.sseDb);
        } catch (CosmianException e) {
            throw new AppException("failed querying the SSE index: " + e.getMessage(), e);
        }
        final long searchTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - then);

        logger.fine(() -> "found results for " + result.size() + " word(s)");

        Set<DbUid> set = new HashSet<>();
        for (Set<DbUid> s : result.values()) {
            set.addAll(s);
        }

        logger.fine(() -> "found " + set.size() + " UIDs matching the search");

        OutputDirectory output = OutputDirectory.parse(outputDirectory);
        try (OutputStream os = output.getFs().getOutputStream(output.getDirectory().resolve(outputFilename).toString(),
                true)) {

            long decryptionTime = 0;
            long numEntries = 0;
            for (DbUid uid : set) {
                String fileName = Base58.encode(uid.bytes());
                long time = this.decipher.decryptAtPath(os, this.fsRootPath.getFs(), this.fsRootPath.resolve(fileName));
                if (time > 0) {
                    decryptionTime += time;
                    numEntries += 1;
                }
            }

            final long totalTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - then);
            final long totalEntries = numEntries;
            final long totalDecryptionTime = decryptionTime;
            logger.info(
                    () -> "Search: found " + set.size() + " record(s) and decrypted " + totalEntries + " record(s) in "
                            + totalTime + "ms (search time: " + (searchTime * 100 / totalTime)
                            + "%, ABE decryption time: " + (totalDecryptionTime * 100 / totalTime) + "% )");
            if (totalEntries > 0) {
                logger.info("Search: average decryption time: " + (totalDecryptionTime / totalEntries) + "ms/record");
            }

        } catch (IOException e) {
            throw new AppException("failed opening output file: " + outputFilename + ": " + e.getMessage(), e);
        }

    }

    @Override
    public void close() {
        this.decipher.close();
        this.sseDb.close();
    }

}
