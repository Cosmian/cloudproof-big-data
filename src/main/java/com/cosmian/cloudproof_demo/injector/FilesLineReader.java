package com.cosmian.cloudproof_demo.injector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import com.cosmian.cloudproof_demo.AppException;
import com.cosmian.cloudproof_demo.fs.InputPath;

/**
 * Reads line by line data from a list of input paths
 */
public class FilesLineReader implements LineReader {

    private static final Logger logger = Logger.getLogger(FilesLineReader.class.getName());

    final Iterator<String> inputPaths;

    transient InputPath inputPath;

    transient Iterator<String> inputPathFiles;

    transient String inputFile;

    transient BufferedReader br;

    public FilesLineReader(List<String> inputPaths) {
        this.inputPaths = inputPaths.iterator();
        br = null;
        inputPathFiles = null;
        inputPath = null;
    }

    @Override
    public String readNext() throws AppException {
        if (br == null) {
            return readNexInputPathFile();
        }
        String line;
        try {
            line = br.readLine();
        } catch (IOException e) {
            throw new AppException("an /IO Error occurred reading " + inputFile + ":" + e.getMessage(), e);
        }
        if (line != null)
            return line;
        // check if there is another file in the input
        return readNexInputPathFile();
    }

    private String readNexInputPathFile() throws AppException {
        closeBufferedReader();
        if (inputPathFiles == null || !inputPathFiles.hasNext()) {
            return readNextInputPath();
        }
        inputFile = inputPathFiles.next();
        logger.fine(() -> "Starting the processing of input file: " + inputFile);
        InputStream is = inputPath.getFs().getInputStream(inputFile);
        br = new BufferedReader(new InputStreamReader(is));
        return readNext();
    }

    private String readNextInputPath() throws AppException {
        if (inputPaths.hasNext()) {
            inputPath = InputPath.parse(inputPaths.next());
            inputPathFiles = inputPath.listFiles();
            return readNexInputPathFile();
        }
        // done
        return null;
    }

    @Override
    public void close() {
        closeBufferedReader();
    }

    private void closeBufferedReader() {
        if (br != null) {
            try {
                br.close();
            } catch (IOException e) {
                logger.severe("Failed closing the file: " + inputFile + ": " + e.getMessage());
            }
        }
    }

}
