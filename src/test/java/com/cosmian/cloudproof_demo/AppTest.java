package com.cosmian.cloudproof_demo;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.cosmian.CosmianException;
import com.cosmian.RestClient;
import com.cosmian.cloudproof_demo.extractor.StandaloneExtractor;
import com.cosmian.cloudproof_demo.injector.StandaloneInjector;
import com.cosmian.cloudproof_demo.policy.Country;
import com.cosmian.cloudproof_demo.policy.DemoPolicy;
import com.cosmian.cloudproof_demo.policy.Department;
import com.cosmian.cloudproof_demo.search.StandaloneSearch;
import com.cosmian.cloudproof_demo.sse.Sse.Key;
import com.cosmian.cloudproof_demo.sse.Sse.Word;
import com.cosmian.jna.FfiException;
import com.cosmian.jna.cover_crypt.DecryptedHeader;
import com.cosmian.jna.cover_crypt.EncryptedHeader;
import com.cosmian.jna.cover_crypt.Ffi;
import com.cosmian.rest.cover_crypt.CoverCrypt;
import com.cosmian.rest.cover_crypt.acccess_policy.And;
import com.cosmian.rest.cover_crypt.acccess_policy.Attr;
import com.cosmian.rest.cover_crypt.acccess_policy.Or;
import com.cosmian.rest.cover_crypt.policy.Policy;
import com.cosmian.rest.kmip.objects.PrivateKey;
import com.cosmian.rest.kmip.objects.PublicKey;

public class AppTest {

    @BeforeAll
    public static void before_all() {
        App.configureLog4j();
        App.initLogging(Level.INFO);
        final Logger logger = Logger.getLogger("com.cosmian.cloudproof_demo");
        logger.setLevel(Level.FINE);
    }

    @Test
    public void printUsage() throws Exception {
        App.main(new String[] {});
    }

    @Test
    public void generateSymmetricKeys() throws Exception {
        String wd = Paths.get("").toAbsolutePath().toString();
        String path = wd + "/src/test/resources/keys";
        KeyGenerator gen = new KeyGenerator(path);
        gen.generateSymmetricKeys();
        gen.generateAsymmetricKeys();
        File k = Paths.get(path, KeyGenerator.SSE_K_KEY_FILENAME).toFile();
        assertTrue(k.exists());
        assertEquals(32, k.length());
        File k_star = Paths.get(path, KeyGenerator.SSE_K_STAR_KEY_FILENAME).toFile();
        assertTrue(k_star.exists());
        assertEquals(32, k_star.length());
    }

    @Test
    public void testApp() throws Exception {
        Path resourcesPath = Paths.get("").toAbsolutePath().resolve("src/test/resources");

        if (TestUtils.serverAvailable(TestUtils.kmsServerUrl())) {
            KeyGenerator kg = new KeyGenerator(resourcesPath.resolve("keys").toString());
            kg.generateKeys();
        }

        // encrypting
        Path publicKeyFile = resourcesPath.resolve("keys/public_key.json");
        String publicKeyJson;
        try {
            publicKeyJson = LocalResource.load_file_string(publicKeyFile.toFile());
        } catch (IOException e) {
            throw new AppException("Failed loading the public key file:" + e.getMessage(), e);
        }

        Key k = CliParser.k(publicKeyFile.getParent());
        Key kStar = CliParser.kStar(publicKeyFile.getParent());

        Path inputFile = resourcesPath.resolve("line.txt");
        List<String> inputs = Arrays.asList(new String[] {inputFile.toString()});
        Path encryptedOutputDirectory = Files.createTempDirectory(null);

        // encryption
        new StandaloneInjector().run(k, kStar, publicKeyJson, encryptedOutputDirectory.toString(),
            new DseDB.Configuration(), inputs);

        String encryptedFileName =
            Stream.of(new File(encryptedOutputDirectory.toString()).listFiles()).map(File::getName).findFirst().get();

        // decrypting
        Path decryptedOutputDirectory = Files.createTempDirectory(null); // resourcesPath.resolve("dec");
        Path privateKeyFile = resourcesPath.resolve("keys/user_Alice_key.json");
        String privateKeyJson;
        try {
            privateKeyJson = LocalResource.load_file_string(privateKeyFile.toFile());
        } catch (IOException e) {
            throw new AppException("Failed loading the private key file:" + e.getMessage(), e);
        }

        Path encryptedInputFile = encryptedOutputDirectory.resolve(encryptedFileName);
        List<String> encryptedInputs = Arrays.asList(new String[] {encryptedInputFile.toString()});
        long totalEntries = new StandaloneExtractor().run(encryptedInputs, privateKeyJson,
            decryptedOutputDirectory.toString(), "CT-" + encryptedFileName);
        assertEquals(1, totalEntries);

        // search
        String[] wordsList = new String[] {"country=France"};
        Set<Word> words = new HashSet<Word>();
        for (int i = 1; i < wordsList.length; i++) {
            words.add(new Word(wordsList[i].toLowerCase().getBytes(StandardCharsets.UTF_8)));
        }
        long[] results = new StandaloneSearch().run(words, false, encryptedOutputDirectory.toString(), k,
            privateKeyJson, new DseDB.Configuration(), decryptedOutputDirectory.toString(), "SE-" + encryptedFileName);

        assertEquals(1, results[0], "should have found 1 entry");
        assertEquals(1, results[1], "should have decrypted 1 entry");

    }

    @Test
    public void testCoverCrypt() throws Exception {

        if (!TestUtils.serverAvailable(TestUtils.kmsServerUrl())) {
            System.out.println("KMS Server not available; skipping test");
            return;
        }

        Policy policy = DemoPolicy.getInstance();

        CoverCrypt cc = new CoverCrypt(new RestClient(TestUtils.kmsServerUrl(), TestUtils.apiKey()));
        String[] masterKeys = cc.createMasterKeyPair(policy);

        // Recover the public key
        String publicMasterKeyUid = masterKeys[1];
        PublicKey publicKey = cc.retrievePublicMasterKey(publicMasterKeyUid);
        String privateMasterKeyUid = masterKeys[0];

        // Test no UID
        {
            // Encrypt some data
            EncryptedHeader encryptedHeader = Ffi.encryptHeader(publicKey,
                new Attr[] {Department.Marketing.getAttribute(), Country.France.getAttribute()});
            // direct access
            {
                String userKeyId = cc.createUserDecryptionKey(
                    new And(Department.Marketing.getAttribute(), Country.France.getAttribute()), privateMasterKeyUid);
                PrivateKey userKey = cc.retrieveUserDecryptionKey(userKeyId);

                DecryptedHeader header = Ffi.decryptHeader(userKey, encryptedHeader.getEncryptedHeaderBytes());
                assertArrayEquals(encryptedHeader.getSymmetricKey(), header.getSymmetricKey());
            }
            // no access
            {
                String userKeyId = cc.createUserDecryptionKey(
                    new And(Department.Other.getAttribute(), Country.France.getAttribute()), privateMasterKeyUid);
                PrivateKey userKey = cc.retrieveUserDecryptionKey(userKeyId);

                try {
                    Ffi.decryptHeader(userKey, encryptedHeader.getEncryptedHeaderBytes());
                    throw new RuntimeException("This should have failed");
                } catch (FfiException | CosmianException e) {
                    // fine
                }
            }
            // more complex access policy
            {
                String userKeyId = cc.createUserDecryptionKey(new And(Department.Marketing.getAttribute(),
                    new Or(Country.Spain.getAttribute(), Country.France.getAttribute())), privateMasterKeyUid);
                PrivateKey userKey = cc.retrieveUserDecryptionKey(userKeyId);

                DecryptedHeader header = Ffi.decryptHeader(userKey, encryptedHeader.getEncryptedHeaderBytes());
                assertArrayEquals(encryptedHeader.getSymmetricKey(), header.getSymmetricKey());
            }
            {
                // block
                byte[] data = new byte[] {1, 2, 3, 4, 5};
                byte[] encryptedBlock = Ffi.encryptBlock(encryptedHeader.getSymmetricKey(), data);
                byte[] decryptedBlock = Ffi.decryptBlock(encryptedHeader.getSymmetricKey(), encryptedBlock);
                assertArrayEquals(data, decryptedBlock);
            }
        }

        // Test with UID
        {
            byte[] uid = new byte[] {23, 43, 12, 43, 89, 121, 0};
            // Encrypt some data
            EncryptedHeader encryptedHeader = Ffi.encryptHeader(publicKey,
                new Attr[] {Department.Marketing.getAttribute(), Country.France.getAttribute()}, Optional.of(uid),
                Optional.empty());
            // direct access
            {
                String userKeyId = cc.createUserDecryptionKey(
                    new And(Department.Marketing.getAttribute(), Country.France.getAttribute()), privateMasterKeyUid);
                PrivateKey userKey = cc.retrieveUserDecryptionKey(userKeyId);

                DecryptedHeader header =
                    Ffi.decryptHeader(userKey, encryptedHeader.getEncryptedHeaderBytes(), uid.length, 0);
                assertArrayEquals(encryptedHeader.getSymmetricKey(), header.getSymmetricKey());
                assertArrayEquals(uid, header.getUid());
            }
            // no access
            {
                String userKeyId = cc.createUserDecryptionKey(
                    new And(Department.Other.getAttribute(), Country.France.getAttribute()), privateMasterKeyUid);
                PrivateKey userKey = cc.retrieveUserDecryptionKey(userKeyId);

                try {
                    Ffi.decryptHeader(userKey, encryptedHeader.getEncryptedHeaderBytes(), uid.length, 0);
                    throw new RuntimeException("This should have failed");
                } catch (FfiException | CosmianException e) {
                    // fine
                }
            }
            // more complex access policy
            {
                String userKeyId = cc.createUserDecryptionKey(new And(Department.Marketing.getAttribute(),
                    new Or(Country.Spain.getAttribute(), Country.France.getAttribute())), privateMasterKeyUid);
                PrivateKey userKey = cc.retrieveUserDecryptionKey(userKeyId);

                DecryptedHeader header =
                    Ffi.decryptHeader(userKey, encryptedHeader.getEncryptedHeaderBytes(), uid.length, 0);
                assertArrayEquals(encryptedHeader.getSymmetricKey(), header.getSymmetricKey());
                assertArrayEquals(uid, header.getUid());
            }
            {
                // block
                byte[] data = new byte[] {1, 2, 3, 4, 5};
                byte[] encryptedBlock = Ffi.encryptBlock(encryptedHeader.getSymmetricKey(), uid, 0, data);
                byte[] decryptedBlock = Ffi.decryptBlock(encryptedHeader.getSymmetricKey(), uid, 0, encryptedBlock);
                assertArrayEquals(data, decryptedBlock);
            }
        }
        // using cache
        {
            byte[] uid = new byte[] {23, 43, 12, 43, 89, 121, 0};
            int encryptionCache = Ffi.createEncryptionCache(policy, publicKey.bytes());
            // Encrypt some data
            EncryptedHeader encryptedHeader = Ffi.encryptHeaderUsingCache(encryptionCache,
                new Attr[] {Department.Marketing.getAttribute(), Country.France.getAttribute()}, Optional.of(uid),
                Optional.empty());
            Ffi.destroyEncryptionCache(encryptionCache);
            // direct access
            {
                String userKeyId = cc.createUserDecryptionKey(
                    new And(Department.Marketing.getAttribute(), Country.France.getAttribute()), privateMasterKeyUid);
                PrivateKey userKey = cc.retrieveUserDecryptionKey(userKeyId);
                int decryptionCache = Ffi.createDecryptionCache(userKey);
                DecryptedHeader header = Ffi.decryptHeaderUsingCache(decryptionCache,
                    encryptedHeader.getEncryptedHeaderBytes(), uid.length, 0);
                Ffi.destroyDecryptionCache(decryptionCache);
                assertArrayEquals(encryptedHeader.getSymmetricKey(), header.getSymmetricKey());
                assertArrayEquals(uid, header.getUid());
            }
            // no access
            {
                String userKeyId = cc.createUserDecryptionKey(
                    new And(Department.Other.getAttribute(), Country.France.getAttribute()), privateMasterKeyUid);
                PrivateKey userKey = cc.retrieveUserDecryptionKey(userKeyId);
                int decryptionCache = Ffi.createDecryptionCache(userKey);
                try {
                    Ffi.decryptHeaderUsingCache(decryptionCache, encryptedHeader.getEncryptedHeaderBytes(), uid.length,
                        0);
                    throw new RuntimeException("This should have failed");
                } catch (FfiException e) {
                    // fine
                }
            }
            // more complex access policy
            {
                String userKeyId = cc.createUserDecryptionKey(new And(Department.Marketing.getAttribute(),
                    new Or(Country.Spain.getAttribute(), Country.France.getAttribute())), privateMasterKeyUid);
                PrivateKey userKey = cc.retrieveUserDecryptionKey(userKeyId);
                int decryptionCache = Ffi.createDecryptionCache(userKey);
                DecryptedHeader header = Ffi.decryptHeaderUsingCache(decryptionCache,
                    encryptedHeader.getEncryptedHeaderBytes(), uid.length, 0);
                Ffi.destroyDecryptionCache(decryptionCache);
                assertArrayEquals(encryptedHeader.getSymmetricKey(), header.getSymmetricKey());
                assertArrayEquals(uid, header.getUid());
            }
            {
                // block
                byte[] data = new byte[] {1, 2, 3, 4, 5};
                byte[] encryptedBlock = Ffi.encryptBlock(encryptedHeader.getSymmetricKey(), uid, 0, data);
                byte[] decryptedBlock = Ffi.decryptBlock(encryptedHeader.getSymmetricKey(), uid, 0, encryptedBlock);
                assertArrayEquals(data, decryptedBlock);
            }
        }

    }

}
