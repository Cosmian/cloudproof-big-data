package com.cosmian.cloudproof_demo;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.cosmian.cloudproof_demo.extractor.SparkExtractor;
import com.cosmian.cloudproof_demo.injector.SparkInjector;
import com.cosmian.cloudproof_demo.search.SparkSearch;

public class Spark {

    private static final Logger logger = Logger.getLogger(Spark.class.getName());

    /**
     * The main entry point for a Spark run
     * 
     * @param args
     */
    public static void main(String[] args) throws AppException {

        initLogging(Level.INFO);
        logger.info("Stating session with args: " + Arrays.toString(args));

        SparkConf conf = new SparkConf().setAppName("Cosmian BNPP Application"); // .setMaster(master);
        JavaSparkContext spark = new JavaSparkContext(conf);

        CliParser.parse(args, new SparkInjector(spark), new SparkExtractor(spark), new SparkSearch(spark));

    }

    public static void initLogging(Level level) {
        final java.util.logging.Logger logger = java.util.logging.Logger.getLogger("com.cosmian");
        logger.setLevel(level);
        ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter(new SimpleFormatter());
        handler.setLevel(java.util.logging.Level.ALL);
        logger.setUseParentHandlers(false);
        logger.addHandler(handler);
        logger.fine("Logger was setup");
    }

    /**
     * Sets the Hadoop Configuration if need be and returns the path on the file system
     * 
     * @param spark
     * @param pathUri
     * @return
     */
    public static String setHadoopConf(JavaSparkContext spark, String pathUri) throws AppException {

        URI uri;
        try {
            uri = new URI(pathUri);
        } catch (URISyntaxException e1) {
            throw new AppException("invalid input file URI: " + pathUri + ": " + e1.getMessage(), e1);
        }

        if (uri.getScheme().equals("hdfs")) {
            try {
                String defaultFS = new URI("hdfs", uri.getAuthority(), null, null, null).toString();
                Configuration conf = spark.hadoopConfiguration();
                conf.set("fs.defaultFS", defaultFS);
                // conf.set("hadoop.job.ugi", uri.getUserInfo());
            } catch (URISyntaxException e) {
                throw new AppException("cannot rebuild HDFS URL from: " + pathUri + ": " + e.getMessage(), e);
            }
        } else if (uri.getScheme().equals("hdfsk")) {
            try {
                String defaultFS = new URI(uri.getScheme(), uri.getAuthority(), null, null, null).toString();
                Configuration conf = spark.hadoopConfiguration();
                conf.set("fs.defaultFS", defaultFS);
                conf.set("hadoop.security.authentication", "kerberos");
                conf.set("hadoop.job.ugi", uri.getUserInfo());
                UserGroupInformation.setConfiguration(conf);
            } catch (URISyntaxException e) {
                throw new AppException("canot rebuild HDFS URL from: " + pathUri + ": " + e.getMessage(), e);
            }
        }
        return uri.getPath();
    }

}
