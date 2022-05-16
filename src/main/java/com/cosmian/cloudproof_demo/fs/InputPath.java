package com.cosmian.cloudproof_demo.fs;

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.cosmian.cloudproof_demo.AppException;

/**
 * {@link InputPath} provides an abstraction to list and read files over the
 * various {@link AppFileSystem}
 */
public class InputPath {

    final String inputPath;

    final AppFileSystem fs;

    public InputPath(String inputPath, AppFileSystem fs) {
        this.inputPath = inputPath;
        this.fs = fs;
    }

    public static InputPath parse(String inputPathUri) throws AppException {

        URI uri;
        try {
            uri = new URI(inputPathUri);
        } catch (URISyntaxException e1) {
            throw new AppException("invalid input file URI: " + inputPathUri + ": " + e1.getMessage(), e1);
        }

        AppFileSystem fs;
        if (uri.getScheme() == null || uri.getScheme().equals("file")) {
            fs = new LocalFileSystem();
        } else if (uri.getScheme().equals("hdfs")) {
            try {
                fs = new HDFS(new URI(uri.getScheme(), uri.getAuthority(), null, null, null).toString(),
                        uri.getUserInfo());
            } catch (URISyntaxException e) {
                throw new AppException("cannot rebuild HDFS URL from: " + inputPathUri + ": " + e.getMessage(), e);
            }
        } else if (uri.getScheme().equals("hdfso")) {
            try {
                fs = new HDFSOriginal(new URI("hdfs", uri.getAuthority(), null, null, null).toString(),
                        uri.getUserInfo());
            } catch (URISyntaxException e) {
                throw new AppException(
                        "canot rebuild HDFS URL from: " + inputPathUri + ": " + e.getMessage(), e);
            }
        } else {
            throw new AppException("unknown scheme for the input: " + inputPathUri);
        }

        return new InputPath(uri.getPath(), fs);
    }

    public String getInputPath() {
        return this.inputPath;
    }

    public AppFileSystem getFs() {
        return this.fs;
    }

    public boolean isDirectory() throws AppException {
        return this.fs.isDirectory(this.inputPath);
    }

    public Iterator<String> listFiles() throws AppException {
        Iterator<String> it;
        if (this.isDirectory()) {
            it = this.fs.listFiles(this.inputPath);
        } else {
            List<String> list = new ArrayList<>();
            list.add(this.inputPath);
            it = list.iterator();
        }
        return it;
    }

    public byte[] readFile(String absolutePath) throws AppException {
        return this.getFs().readFile(absolutePath);
    }

    public InputStream getInputStream(String absolutePath) throws AppException {
        return this.getFs().getInputStream(absolutePath);
    }

    /**
     * Resolves the absolute Path by following the same semantics as
     * {@link Paths#get(String, String...)}
     * 
     * @param relativePath
     * @return the absolute path
     */
    public String resolve(String relativePath) {
        return Paths.get(this.inputPath, relativePath).toAbsolutePath().toString();
    }

}
