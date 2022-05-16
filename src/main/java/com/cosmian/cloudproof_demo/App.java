package com.cosmian.cloudproof_demo;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.SimpleFormatter;

import com.cosmian.CosmianException;
import com.cosmian.cloudproof_demo.sse.Sse.Word;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class App {

    public static void main(String[] args) throws AppException {
        // This is due to the HDFS connector. Nothing else below uses Log4j
        configureLog4j();

        // init Java logging
        initLogging(Level.INFO);

        run(args);
    }

    public static void run(String[] args) throws AppException {

        Options cliOptions = App.cliOptions();
        CommandLine cli;
        try {
            cli = new DefaultParser().parse(cliOptions, args);
        } catch (ParseException e) {
            printUsage();
            return;
        }

        String outputDirString = null;
        if (cli.hasOption("output-dir")) {
            outputDirString = cli.getOptionValue("output-dir");
        }

        if (cli.hasOption("generate-keys")) {
            if (outputDirString == null) {
                throw new AppException("The output directory must be specified for key generation");
            }
            try {
                new KeyGenerator(outputDirString).generateKeys();
            } catch (CosmianException e) {
                throw new AppException("Key Generation failed: " + e.getMessage(), e);
            }
            return;
        }

        String clearTextFilename = "clear.txt";
        if (cli.hasOption("clear-text-filename")) {
            clearTextFilename = cli.getOptionValue("clear-text-filename");
        }

        String dseIP = "127.0.0.1";
        if (cli.hasOption("dse-ip")) {
            dseIP = cli.getOptionValue("dse-ip");
        }
        int dsePort = 9042;
        if (cli.hasOption("dse-port")) {
            dsePort = Integer.parseInt(cli.getOptionValue("dse-port"), 10);
        }
        String dseDatacenter = null;
        if (cli.hasOption("dse-datacenter")) {
            dseDatacenter = cli.getOptionValue("dse-datacenter");
        } else if (dseIP == "127.0.0.1") {
            dseDatacenter = "dc1";
        }
        String dseUsername = null;
        if (cli.hasOption("dse-username")) {
            dseUsername = cli.getOptionValue("dse-username");
        }
        String dsePassword = null;
        if (cli.hasOption("dse-password")) {
            dsePassword = cli.getOptionValue("dse-password");
        }
        DseDB.Configuration dseConf = new DseDB.Configuration(dseIP, dsePort, dseDatacenter, dseUsername, dsePassword);

        String keyString = "key.json";
        if (cli.hasOption("key")) {
            keyString = cli.getOptionValue("key");
        }
        Path abeKey = Paths.get(keyString);
        if (!Files.exists(abeKey)) {
            throw new AppException("The key file: " + keyString + " does not exists");
        }
        if (Files.isDirectory(abeKey)) {
            throw new AppException("The key file: " + keyString + " is a directory");
        }
        if (!Files.isReadable(abeKey)) {
            throw new AppException("The key file: " + keyString + " is not readable");
        }

        if (cli.hasOption("encrypt")) {
            Path keysDirectory = abeKey.getParent();
            File kFile = keysDirectory.resolve(KeyGenerator.SSE_K_KEY_FILENAME).toFile();
            if (!kFile.exists()) {
                throw new AppException("The SSE K key does not exists at " + kFile.getAbsolutePath());
            }
            if (kFile.length() != 32) {
                throw new AppException("Invalid SSE K key");
            }
            File kStarFile = keysDirectory.resolve(KeyGenerator.SSE_K_STAR_KEY_FILENAME).toFile();
            if (!kStarFile.exists()) {
                throw new AppException("The SSE K* key does not exists at " + kStarFile.getAbsolutePath());
            }
            if (kStarFile.length() != 32) {
                throw new AppException("Invalid SSE K* key");
            }
            List<String> fileNames = cli.getArgList();
            if (fileNames.size() == 0) {
                throw new AppException("Please supply at least one file/directory to encrypt");
            }
            List<Path> files = new ArrayList<>();
            for (String fileName : fileNames) {
                Path file = Paths.get(fileName);
                if (!Files.exists(file)) {
                    throw new AppException("The data file/directory: " + fileName + " does not exists");
                }
                if (!Files.isReadable(file)) {
                    throw new AppException("The data file/directory: " + fileName + " is not readable");
                }
                files.add(file);
            }
            try (Cipher cipher = new Cipher(abeKey, fileNames, outputDirString, dseConf)) {
                cipher.run();
            }
            return;
        }

        if (cli.hasOption("decrypt")) {
            Path keysDirectory = abeKey.getParent();
            File kFile = keysDirectory.resolve(KeyGenerator.SSE_K_KEY_FILENAME).toFile();
            if (!kFile.exists()) {
                throw new AppException("The SSE K key does not exists at " + kFile.getAbsolutePath());
            }
            if (kFile.length() != 32) {
                throw new AppException("Invalid SSE K key");
            }
            List<String> fileNames = cli.getArgList();
            if (fileNames.size() == 0) {
                throw new AppException("Please supply at least one file/directory to decrypt");
            }
            try (Decipher decipher = new Decipher(abeKey)) {
                decipher.run(fileNames, outputDirString, clearTextFilename);
            }
            return;
        }

        if (cli.hasOption("search")) {
            Path keysDirectory = abeKey.getParent();
            File kFile = keysDirectory.resolve(KeyGenerator.SSE_K_KEY_FILENAME).toFile();
            if (!kFile.exists()) {
                throw new AppException("The SSE K key does not exists at " + kFile.getAbsolutePath());
            }
            if (kFile.length() != 32) {
                throw new AppException("Invalid SSE K key");
            }
            List<String> argsList = cli.getArgList();
            if (argsList.size() == 0) {
                throw new AppException("Please supply a source root URI and at least on search term");
            }
            String fsRootUri = argsList.get(0);
            if (argsList.size() < 2) {
                throw new AppException("Please supply at least on search term");
            }
            Set<Word> words = new HashSet<>();
            for (int i = 1; i < argsList.size(); i++) {
                words.add(new Word(argsList.get(i).toLowerCase().getBytes(StandardCharsets.UTF_8)));
            }
            try (Search search = new Search(abeKey, dseConf, fsRootUri)) {
                search.run(words, outputDirString, clearTextFilename);
            }
            return;
        }

    }

    static Options cliOptions() {
        Options options = new Options();
        OptionGroup group = new OptionGroup();
        group.setRequired(true);
        group.addOption(Option.builder("g").longOpt("generate-keys").desc("generate all the keys").build());
        group.addOption(
                Option.builder("e").longOpt("encrypt").desc("encrypt the supplied files and directories URI(s)")
                        .build());
        group.addOption(
                Option.builder("d").longOpt("decrypt").desc("decrypt the supplied files and directories URI(s)")
                        .build());
        group.addOption(
                Option.builder("s").longOpt("search").desc("search the supplied root URI for the words").build());
        options.addOptionGroup(group);
        options.addOption(Option.builder("k").longOpt("key").argName("FILE").hasArg()
                .desc("the path to the key file: defaults to key.json").required(false).build());
        options.addOption(Option.builder("o").longOpt("output-dir").argName("URI").hasArg()
                .desc("the path of the output directory. Defaults to '.' for the filesystem, /user/${user} for HDFS")
                .required(false).build());
        options.addOption(Option.builder("c").longOpt("clear-text-filename").argName("FILENAME").hasArg()
                .desc("the name of the clear text file when running decryption. Defaults to clear.txt").required(false)
                .build());
        options.addOption(Option.builder("di").longOpt("dse-ip").argName("IP").hasArg()
                .desc("the IP address of the DSE server. Defaults to 127.0.0.1").required(false).build());
        options.addOption(Option.builder("dp").longOpt("dse-port").argName("PORT").hasArg()
                .desc("the port of the DSE server. Defaults to 9042").required(false).build());
        options.addOption(Option.builder("dc").longOpt("dse-datacenter").argName("DATACENTER").hasArg()
                .desc("the datacenter of the DSE server. Defaults to NULL or dc1 if the IP is 127.0.0.1")
                .required(false).build());
        options.addOption(Option.builder("du").longOpt("dse-username").argName("USERNAME").hasArg()
                .desc("the username to connect to the DSE server. Defaults to NULL")
                .required(false).build());
        options.addOption(Option.builder("dup").longOpt("dse-password").argName("PASSWORD").hasArg()
                .desc("the password to connect to the DSE server. Defaults to NULL")
                .required(false).build());
        return options;
    }

    static void printUsage() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("usage: app SUB-COMMAND [OPTIONS] [SOURCE URI] [WORD1, WORD2,...]", cliOptions());
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
