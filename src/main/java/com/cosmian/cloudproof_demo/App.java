package com.cosmian.cloudproof_demo;

import java.util.Arrays;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import com.cosmian.cloudproof_demo.extractor.StandaloneExtractor;
import com.cosmian.cloudproof_demo.injector.StandaloneInjector;
import com.cosmian.cloudproof_demo.search.StandaloneSearch;

public class App {

    private static final Logger logger = Logger.getLogger(App.class.getName());

    /**
     * The main entry point of the binary
     * 
     * @param args
     * @throws AppException
     */
    public static void main(String[] args) throws AppException {

        // This is due to the HDFS connector. Nothing else below uses Log4j
        configureLog4j();

        // init Java logging
        initLogging(Level.INFO);
        logger.info("Stating standalone app with args: " + Arrays.toString(args));

        CliParser.parse(args, new StandaloneInjector(), new StandaloneExtractor(), new StandaloneSearch());

    }

    public static void configureLog4j() {
        org.apache.log4j.ConsoleAppender console = new org.apache.log4j.ConsoleAppender(); // create appender
        // configure the appender
        String PATTERN = "%d [%p|%c|%C{1}] %m%n";
        console.setLayout(new org.apache.log4j.PatternLayout(PATTERN));
        console.setThreshold(org.apache.log4j.Level.FATAL);
        console.activateOptions();
        // add appender to any Logger (here is root)
        org.apache.log4j.Logger.getRootLogger().addAppender(console);
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
}
