package com.cosmian.cloudproof_demo.fs;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.cosmian.cloudproof_demo.AppException;

public class OutputDirectory {

    final AppFileSystem fs;
    final Path directory;

    public OutputDirectory(AppFileSystem fs, Path directory) {
        this.fs = fs;
        this.directory = directory;
    }

    public static OutputDirectory parse(String uriString) throws AppException {

        URI uri;
        try {
            uri = new URI(uriString);
        } catch (URISyntaxException e) {
            throw new AppException("invalid URI for the output directory: " + uriString + ": " + e.getMessage(),
                    e);
        }
        Path directory = Paths.get(uri.getPath());

        AppFileSystem fs;
        if (uri.getScheme() == null || uri.getScheme().equals("file")) {
            fs = new LocalFileSystem();
        } else if (uri.getScheme().equals("hdfs")) {
            try {
                fs = new HDFS(new URI(uri.getScheme(), uri.getAuthority(), null, null, null).toString(),
                        uri.getUserInfo());
            } catch (URISyntaxException e) {
                throw new AppException(
                        "canot rebuild HDFS URL from: " + uriString + ": " + e.getMessage(), e);
            }
        } else if (uri.getScheme().equals("hdfso")) {
            try {
                fs = new HDFSOriginal(new URI("hdfs", uri.getAuthority(), null, null, null).toString(),
                        uri.getUserInfo());
            } catch (URISyntaxException e) {
                throw new AppException(
                        "canot rebuild HDFS URL from: " + uriString + ": " + e.getMessage(), e);
            }
        } else {
            throw new AppException("unknown scheme for the output directory: " + uriString);
        }

        if (!fs.isDirectory(directory.toString())) {
            throw new AppException("not a directory: " + uriString);
        }

        return new OutputDirectory(fs, directory);
    }

    public AppFileSystem getFs() {
        return this.fs;
    }

    public Path getDirectory() {
        return this.directory;
    }

}
