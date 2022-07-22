package com.cosmian.cloudproof_demo;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.PrivilegedExceptionAction;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.cosmian.cloudproof_demo.fs.HDFS;

public class HDFSTest {

    private static final Logger logger = Logger.getLogger(HDFSTest.class.getName());

    private final static String hdfsUrl = "hdfs://root@localhost:9000/";
    static {
        App.configureLog4j();
    }

    @BeforeAll
    public static void before_all() {
        App.initLogging(Level.INFO);
    }

    @Test
    public void testHDFS() throws Exception {
        if (!HdfsAvailable(hdfsUrl, "root")) {
            System.out.println("HDFS not present !");
            return;
        }

        byte[] data = "Hello, World".getBytes(StandardCharsets.UTF_8);

        Path parent = Paths.get("/user/root");
        HDFS hdfs = new HDFS(hdfsUrl, "root");
        hdfs.writeFile(parent.resolve("hw").toString(), data);
        byte[] data_ = hdfs.readFile(parent.resolve("hw").toString());
        assertArrayEquals(data, data_);

        Iterator<String> it = hdfs.listFiles(parent.toString());
        while (it.hasNext()) {
            String f = it.next();
            logger.fine("Found: " + f + "  " + Paths.get(f).getFileName());
            if (Paths.get(f).getFileName().toString().equals("hw")) {
                return;
            }
        }
        throw new AppException("File not found");

    }

    static boolean HdfsAvailable(String rootHdfsUrl, String user) {
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", rootHdfsUrl);
        conf.set("hadoop.job.ugi", user);

        try {
            return ugi.doAs(new PrivilegedExceptionAction<Boolean>() {

                public Boolean run() throws Exception {

                    try {
                        FileSystem fs = FileSystem.get(conf);
                        fs.getStatus();
                    } catch (IOException e) {
                        logger.info(
                            "Skipping test: HDFS not available at:  " + rootHdfsUrl + " (" + e.getMessage() + ")");
                        return false;
                    }

                    return true;
                }
            });
        } catch (InterruptedException e) {
            return false;
        } catch (IOException e) {
            return false;
        }

    }

    @Test
    void testUrl() throws Exception {
        URI f1 = new URI("/var/run");
        assertEquals(null, f1.getScheme());
        assertEquals("/var/run", f1.getPath());

        URI f2 = new URI("src/main");
        assertEquals(null, f2.getScheme());
        assertEquals("src/main", f2.getPath());

        URI hd = new URI(hdfsUrl);
        assertEquals("hdfs", hd.getScheme());
        assertEquals(hdfsUrl, hd.toString());

    }

}
