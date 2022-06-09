package com.cosmian.cloudproof_demo.fs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.security.UserGroupInformation;

import com.cosmian.cloudproof_demo.AppException;

public class HDFSKerberos implements AppFileSystem {

    final Configuration conf;

    final String user;

    public HDFSKerberos(String rootHdfsUrl, String user) {
        this.user = user;

        conf = new Configuration();
        System.out.println("*** rootHdfsUrl=" + rootHdfsUrl);
        String rootHdfsUrl2 = cut(rootHdfsUrl);
        System.out.println("*** rootHdfsUrl2=" + rootHdfsUrl2);
        conf.set("fs.defaultFS", rootHdfsUrl2);
        System.out.println("*** user=" + user);
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hadoop.job.ugi", user);
        //
        UserGroupInformation.setConfiguration(conf);
    }

    private static String cut(String str) {
        int idx1 = str.indexOf("//");
        int idx2 = str.indexOf("@");
        return str.substring(0, idx1 + 2) + str.substring(idx2 + 1);
    }

    public void writeFile(String filePath, byte[] bytes) throws AppException {

        try {
            FileSystem fs;
            try {
                fs = FileSystem.get(conf);
            } catch (IOException e) {
                throw new AppException("Unable to access the HDFS file system: " + e.getMessage());
            }

            Path p = new Path(filePath);

            try (FSDataOutputStream out = fs.create(p, true)) {
                out.write(bytes);
                out.flush();
            } catch (IOException e) {
                throw new InterruptedException("Failed writing the HDFS file system: " + e.getMessage());
            }
        } catch (InterruptedException e) {
            throw new AppException("failed writing file: " + filePath + " to HDFS : " + e.getMessage(), e);
        }
    }

    public byte[] readFile(String filePath) throws AppException {

        try {
            FileSystem fs;
            try {
                fs = FileSystem.get(conf);
            } catch (IOException e) {
                throw new AppException("Unable to access the HDFS file system: " + e.getMessage());
            }

            Path p = new Path(filePath);

            final int bufferLength = 4096;
            byte[] buffer = new byte[bufferLength];
            ByteArrayOutputStream bao = new ByteArrayOutputStream(bufferLength);
            try (FSDataInputStream in = fs.open(p, bufferLength)) {
                int len;
                while ((len = in.read(buffer)) > 0) {
                    bao.write(buffer, 0, len);
                }
                return bao.toByteArray();
            } catch (IOException e) {
                throw new InterruptedException("Failed reading the HDFS file system: " + e.getMessage());
            }
        } catch (InterruptedException e) {
            throw new AppException("failed reading file: " + filePath + " from HDFS : " + e.getMessage(), e);
        }
    }

    public Iterator<String> listFiles(String directoryPath) throws AppException {

        try {
            FileSystem fs;
            try {
                fs = FileSystem.get(conf);
            } catch (IOException e) {
                throw new AppException("Unable to access the HDFS file system: " + e.getMessage());
            }

            Path p = new Path(directoryPath);
            RemoteIterator<LocatedFileStatus> it = fs.listFiles(p, false);
            return new Iterator<String>() {

                @Override
                public boolean hasNext() {
                    try {
                        return it.hasNext();
                    } catch (IOException e) {
                        throw new RuntimeException("Failed checking the availability of the next HDFS element", e);
                    }
                }

                @Override
                public String next() {
                    LocatedFileStatus lfs;
                    try {
                        lfs = it.next();
                    } catch (IOException e) {
                        throw new RuntimeException("Failed fetching the next HDFS element", e);
                    }
                    return lfs.getPath().toUri().getPath().toString();
                }
            };
        } catch (IOException e) {
            throw new AppException("failed listing HDFS files from: " + directoryPath + ": " + e.getMessage(), e);
        }
    }

    @Override
    public OutputStream getOutputStream(String filePath, boolean overwrite) throws AppException {
        try {
            FileSystem fs;
            try {
                fs = FileSystem.get(conf);
            } catch (IOException e) {
                throw new AppException("Unable to access the HDFS file system: " + e.getMessage());
            }

            Path p = new Path(filePath);

            return fs.create(p, overwrite);
        } catch (IOException e) {
            throw new AppException("failed writing file: " + filePath + " to HDFS : " + e.getMessage(), e);
        }
    }

    @Override
    public boolean isDirectory(String directoryPath) throws AppException {
        try {
            FileSystem fs;
            try {
                fs = FileSystem.get(conf);
            } catch (IOException e) {
                throw new AppException("Unable to access the HDFS file system: " + e.getMessage());
            }

            Path p = new Path(directoryPath);

            return fs.isDirectory(p);
        } catch (IOException e) {
            throw new AppException(
                "failed determining if: " + directoryPath + " is an HDFS directory : " + e.getMessage(), e);
        }
    }

    @Override
    public InputStream getInputStream(String filePath) throws AppException {
        try {
            FileSystem fs;
            try {
                fs = FileSystem.get(conf);
            } catch (IOException e) {
                throw new AppException("Unable to access the HDFS file system: " + e.getMessage());
            }

            Path p = new Path(filePath);

            return fs.open(p);

        } catch (IOException e) {
            throw new AppException("failed reading file: " + filePath + " from HDFS : " + e.getMessage(), e);
        }
    }

    @Override
    public String getPath(String directoryPath, String filename) throws AppException {
        return Paths.get(directoryPath, filename).toString();
    }

}
