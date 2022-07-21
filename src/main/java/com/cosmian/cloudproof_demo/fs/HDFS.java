package com.cosmian.cloudproof_demo.fs;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.security.PrivilegedExceptionAction;
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

public class HDFS implements AppFileSystem {

    final UserGroupInformation ugi;

    final Configuration conf;

    final String user;

    public HDFS(String rootHdfsUrl, String user) {
        this.user = user;
        ugi = UserGroupInformation.createRemoteUser(user);

        conf = new Configuration();
        conf.set("fs.defaultFS", rootHdfsUrl);
        conf.set("hadoop.job.ugi", user);
    }

    public void writeFile(String filePath, byte[] bytes) throws AppException {

        try {
            ugi.doAs(new PrivilegedExceptionAction<Void>() {

                public Void run() throws Exception {

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
                    return null;
                }
            });
        } catch (InterruptedException e) {
            throw new AppException("failed writing file: " + filePath + " to HDFS : " + e.getMessage(), e);
        } catch (IOException e) {
            throw new AppException("failed writing file: " + filePath + " to HDFS : " + e.getMessage(), e);
        }
    }

    public byte[] readFile(String filePath) throws AppException {

        try {
            return ugi.doAs(new PrivilegedExceptionAction<byte[]>() {

                public byte[] run() throws Exception {

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
                }
            });
        } catch (InterruptedException e) {
            throw new AppException("failed reading file: " + filePath + " from HDFS : " + e.getMessage(), e);
        } catch (IOException e) {
            throw new AppException("failed reading file: " + filePath + " from HDFS : " + e.getMessage(), e);
        }
    }

    public Iterator<String> listFiles(String directoryPath) throws AppException {

        try {
            return ugi.doAs(new PrivilegedExceptionAction<Iterator<String>>() {

                public Iterator<String> run() throws Exception {

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
                                throw new RuntimeException("Failed checking the availability of the next HDFS element",
                                    e);
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
                }
            });
        } catch (InterruptedException e) {
            throw new AppException("failed listing HDFS files from: " + directoryPath + ": " + e.getMessage(), e);
        } catch (IOException e) {
            throw new AppException("failed listing HDFS files from: " + directoryPath + ": " + e.getMessage(), e);
        }
    }

    @Override
    public OutputStream getOutputStream(String filePath, boolean overwrite) throws AppException {
        try {
            return ugi.doAs(new PrivilegedExceptionAction<OutputStream>() {

                public OutputStream run() throws Exception {

                    FileSystem fs;
                    try {
                        fs = FileSystem.get(conf);
                    } catch (IOException e) {
                        throw new AppException("Unable to access the HDFS file system: " + e.getMessage());
                    }

                    Path p = new Path(filePath);

                    return fs.create(p, overwrite);

                }
            });
        } catch (InterruptedException e) {
            throw new AppException("failed writing file: " + filePath + " to HDFS : " + e.getMessage(), e);
        } catch (IOException e) {
            throw new AppException("failed writing file: " + filePath + " to HDFS : " + e.getMessage(), e);
        }
    }

    @Override
    public boolean isDirectory(String directoryPath) throws AppException {
        try {
            return ugi.doAs(new PrivilegedExceptionAction<Boolean>() {

                public Boolean run() throws Exception {

                    FileSystem fs;
                    try {
                        fs = FileSystem.get(conf);
                    } catch (IOException e) {
                        throw new AppException("Unable to access the HDFS file system: " + e.getMessage());
                    }

                    Path p = new Path(directoryPath);

                    return fs.isDirectory(p);

                }
            });
        } catch (InterruptedException e) {
            throw new AppException(
                "failed determining if: " + directoryPath + " is an HDFS directory : " + e.getMessage(), e);
        } catch (IOException e) {
            throw new AppException(
                "failed determining if: " + directoryPath + " is an HDFS directory : " + e.getMessage(), e);
        }
    }

    @Override
    public DataInputStream getInputStream(String filePath) throws AppException {
        try {
            return ugi.doAs(new PrivilegedExceptionAction<DataInputStream>() {

                public DataInputStream run() throws Exception {

                    FileSystem fs;
                    try {
                        fs = FileSystem.get(conf);
                    } catch (IOException e) {
                        throw new AppException("Unable to access the HDFS file system: " + e.getMessage());
                    }

                    Path p = new Path(filePath);

                    return fs.open(p);

                }
            });
        } catch (InterruptedException e) {
            throw new AppException("failed reading file: " + filePath + " from HDFS : " + e.getMessage(), e);
        } catch (IOException e) {
            throw new AppException("failed reading file: " + filePath + " from HDFS : " + e.getMessage(), e);
        }
    }

    @Override
    public String getPath(String directoryPath, String filename) throws AppException {
        return Paths.get(directoryPath, filename).toString();
    }

}
