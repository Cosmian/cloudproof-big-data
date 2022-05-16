package com.cosmian.bnpp;

import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class TestUtils {

    public static void initLogging() {
        initLogging(Level.INFO);
    }

    public static void initLogging(Level level) {
        final Logger logger = Logger.getLogger("com.cosmian");
        logger.setLevel(level);
        ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter(new SimpleFormatter());
        handler.setLevel(Level.ALL);
        logger.setUseParentHandlers(false);
        logger.addHandler(handler);
        logger.fine("Logger was setup");
    }

}
