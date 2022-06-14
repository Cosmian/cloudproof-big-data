package com.cosmian.cloudproof_demo;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.cosmian.CosmianException;
import com.cosmian.cloudproof_demo.extractor.Extractor;
import com.cosmian.cloudproof_demo.fs.LocalFileSystem;
import com.cosmian.cloudproof_demo.injector.Injector;
import com.cosmian.cloudproof_demo.search.Search;
import com.cosmian.cloudproof_demo.sse.Sse.Key;
import com.cosmian.cloudproof_demo.sse.Sse.Word;

public class CliParser {

    /**
     * This may be called by the binary or spark
     * 
     * @param args
     * @throws AppException
     */
    public static void parse(String[] args, Injector injector, Extractor extractor, Search search) throws AppException {

        Options cliOptions = cliOptions();
        CommandLine cli;
        try {
            cli = new BasicParser().parse(cliOptions, args);
        } catch (ParseException e) {
            printUsage();
            return;
        }

        String clearTextFilename = "clear.txt";
        if (cli.hasOption("clear-text-filename")) {
            clearTextFilename = cli.getOptionValue("clear-text-filename");
        }

        String outputDirString = null;
        if (cli.hasOption("output-dir")) {
            outputDirString = cli.getOptionValue("output-dir");
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
        String dseKeyspace = null;
        if (cli.hasOption("dse-keyspace")) {
            dseKeyspace = cli.getOptionValue("dse-keyspace");
        }
        DseDB.Configuration dseConf =
            new DseDB.Configuration(dseIP, dsePort, dseDatacenter, dseUsername, dsePassword, dseKeyspace);

        String keyString = "key.json";
        if (cli.hasOption("key")) {
            keyString = cli.getOptionValue("key");
        }
        Path abKey = Paths.get(keyString);
        // not necessary to check for key generation
        if (!cli.hasOption("generate-keys")) {
            if (!Files.exists(abKey)) {
                throw new AppException("The key file: " + keyString + " does not exists");
            }
            if (Files.isDirectory(abKey)) {
                throw new AppException("The key file: " + keyString + " is a directory");
            }
            if (!Files.isReadable(abKey)) {
                throw new AppException("The key file: " + keyString + " is not readable");
            }
        }

        if (cli.hasOption("generate-keys")) {
            if (outputDirString == null) {
                throw new AppException("Key Generation failed: the output directory must be specified ");
            }
            try {
                new KeyGenerator(outputDirString).generateKeys();
            } catch (CosmianException e) {
                throw new AppException("Key Generation failed: " + e.getMessage(), e);
            }
            return;
        }

        if (cli.hasOption("encrypt")) {
            Path keysDirectory = abKey.getParent();

            Key k = k(keysDirectory);
            Key kStar = kStar(keysDirectory);

            String publicKeyJson;
            try {
                publicKeyJson = LocalResource.load_file_string(abKey.toFile());
            } catch (IOException e) {
                throw new AppException("Failed loading the public key file:" + e.getMessage(), e);
            }

            @SuppressWarnings("unchecked")
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
            injector.run(k, kStar, publicKeyJson, outputDirString, dseConf, fileNames);
        }

        if (cli.hasOption("decrypt")) {
            String privateKeyJson;
            try {
                privateKeyJson = LocalResource.load_file_string(abKey.toFile());
            } catch (IOException e) {
                throw new AppException("Failed loading the private key file:" + e.getMessage(), e);
            }

            @SuppressWarnings("unchecked")
            List<String> fileNames = cli.getArgList();
            if (fileNames.size() == 0) {
                throw new AppException("Please supply at least one file/directory to decrypt");
            }
            extractor.run(fileNames, privateKeyJson, outputDirString, clearTextFilename);
            return;
        }

        if (cli.hasOption("search")) {

            boolean disjunction = cli.hasOption("disjunction");

            Path keysDirectory = abKey.getParent();

            Key k = k(keysDirectory);

            String privateKeyJson;
            try {
                privateKeyJson = LocalResource.load_file_string(abKey.toFile());
            } catch (IOException e) {
                throw new AppException("Failed loading the private key file:" + e.getMessage(), e);
            }

            @SuppressWarnings("unchecked")
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
            search.run(words, disjunction, fsRootUri, k, privateKeyJson, dseConf, outputDirString, clearTextFilename);
            return;
        }

    }

    static Options cliOptions() {
        Options options = new Options();
        OptionGroup group = new OptionGroup();
        group.setRequired(true);
        group.addOption(new Option("g", "generate-keys", false, "generate all the keys"));
        group.addOption(new Option("e", "encrypt", false, "encrypt the supplied files and directories URI(s)"));
        group.addOption(new Option("d", "decrypt", false, "decrypt the supplied files and directories URI(s)"));
        group.addOption(new Option("s", "search", false, "search the supplied root URI for the words"));
        options.addOptionGroup(group);
        options.addOption(new Option("or", "disjunction", false,
            "run a disjunction (OR) between the search words. Defaults to conjunction (AND)"));
        options.addOption(new Option("k", "key", true, "the path to the key file: defaults to key.json"));
        options.addOption(new Option("o", "output-dir", true,
            "the path of the output directory. Defaults to '.' for the filesystem, /user/${user} for HDFS"));
        options.addOption(new Option("c", "clear-text-filename", true,
            "the name of the clear text file when running decryption. Defaults to clear.txt"));
        options.addOption(new Option("di", "dse-ip", true, "the IP address of the DSE server. Defaults to 127.0.0.1"));
        options.addOption(new Option("dp", "dse-port", true, "the port of the DSE server. Defaults to 9042"));
        options.addOption(new Option("dc", "dse-datacenter", true,
            "the datacenter of the DSE server. Defaults to NULL or dc1 if the IP is 127.0.0.1"));
        options.addOption(new Option("dk", "dse-keyspace", true,
            "the keyspace to use for the tables. Defaults to NULL in which case th program will attempt to create the cosmian_sse keyspace"));
        options.addOption(
            new Option("du", "dse-username", true, "the username to connect to the DSE server. Defaults to NULL"));
        options.addOption(
            new Option("dup", "dse-password", true, "the password to connect to the DSE server. Defaults to NULL"));
        return options;
    }

    static void printUsage() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("usage: app SUB-COMMAND [OPTIONS] [SOURCE URI] [WORD1, WORD2,...]", cliOptions());
    }

    public static Key k(Path keysDirectory) throws AppException {
        LocalFileSystem fs = new LocalFileSystem();
        File kFile = keysDirectory.resolve(KeyGenerator.SSE_K_KEY_FILENAME).toFile();
        if (!kFile.exists()) {
            throw new AppException("The SSE K key does not exists at " + kFile.getAbsolutePath());
        }
        if (kFile.length() != 32) {
            throw new AppException("Invalid SSE K key");
        }
        return new Key(fs.readFile(kFile));
    }

    public static Key kStar(Path keysDirectory) throws AppException {
        LocalFileSystem fs = new LocalFileSystem();
        File kStarFile = keysDirectory.resolve(KeyGenerator.SSE_K_STAR_KEY_FILENAME).toFile();
        if (!kStarFile.exists()) {
            throw new AppException("The SSE K* key does not exists at " + kStarFile.getAbsolutePath());
        }
        if (kStarFile.length() != 32) {
            throw new AppException("Invalid SSE K* key");
        }
        return new Key(fs.readFile(kStarFile));
    }
}
