package com.cosmian.cloudproof_demo.injector;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.cosmian.cloudproof_demo.AppException;
import com.cosmian.cloudproof_demo.RecordUid;
import com.cosmian.cloudproof_demo.fs.OutputDirectory;

/**
 * Handles the encrypted data output to a file or a new file when the rollover conditions are met
 */
public class OutputFile implements AutoCloseable, Serializable {

    private static final Logger logger = Logger.getLogger(OutputFile.class.getName());

    private final OutputDirectory outputDirectory;

    private final int maxSizeInMB;

    private final int maxAgeInSeconds;

    private String filename;

    private OutputStream os;

    private long mark;

    private long startTime;

    public OutputFile(OutputDirectory outputDirectory, int maxSizeInMB, int maxAgeInSeconds) {
        this.outputDirectory = outputDirectory;
        this.maxSizeInMB = maxSizeInMB;
        this.maxAgeInSeconds = maxAgeInSeconds;
        this.filename = null;
        this.os = null;
        this.mark = 0;
    }

    /**
     * Write to the currently opened stream
     * 
     * @param bytes the bytes to write
     * @throws AppException if no stream is opened or an I/O occurs
     */
    public void write(byte[] bytes) throws AppException {
        if (os == null) {
            throw new AppException("cannot write: no output stream");
        }
        try {
            // The size of the bytes in BE bytes
            ByteBuffer sizeBytes = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(bytes.length);
            // Write the length and the content
            this.os.write(sizeBytes.array());
            this.os.write(bytes);
            this.os.flush();
        } catch (IOException e) {
            String msg = "failed writing to file " + this.filename + ": " + e.getMessage();
            logger.severe(msg);
            throw new AppException(msg, e);
        }
        this.mark += 4 + bytes.length;
    }

    /**
     * Get the next record UID. This will open a stream is none is open or rollover to the next stream if the max-age or
     * max-size parameters are met on the current stream
     * 
     * @return the next record UID
     * @throws AppException if a new stream cannot be opened
     */
    public RecordUid nextRecordUid() throws AppException {
        if (this.os == null) {
            nextOs();
        } else if (shouldRollOver()) {
            try {
                this.os.flush();
                this.os.close();
            } catch (IOException e) {
                logger.severe(
                    "failed closing the file: " + this.filename + ": " + e.getMessage() + ". Continuing anyway...");
            }
            nextOs();
        } else {
            try {
                this.os.flush();
            } catch (IOException e) {
                String msg = "failed flushing file " + this.filename + ": " + e.getMessage();
                logger.severe(msg);
                throw new AppException(msg, e);
            }
        }
        return new RecordUid(filename, mark);
    }

    private boolean shouldRollOver() {

        if (this.mark / (1024 * 1024) >= this.maxSizeInMB) {
            logger.info(() -> "file is exceeding max size of " + this.maxSizeInMB + "MB: rolling over to a new file");
            return true;
        }
        if (TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime) >= this.maxAgeInSeconds) {
            logger.info(
                () -> "file is exceeding the max age of " + this.maxAgeInSeconds + "s: rolling over to a new file");
            return true;
        }
        return false;

    }

    private void nextOs() throws AppException {
        this.filename = UUID.randomUUID().toString();
        String filePath = outputDirectory.getDirectory().resolve(filename).toString();
        this.os = outputDirectory.getFs().getOutputStream(filePath, true);
        this.mark = 0;
        this.startTime = System.nanoTime();
        logger.info(() -> "started new output file: " + filename);
    }

    @Override
    public void close() {
        if (this.os != null) {
            try {
                os.flush();
                os.close();
            } catch (IOException e) {
                String msg = "failed closing file " + this.filename + ": " + e.getMessage();
                logger.severe(msg);
            }
        }
    }

}
