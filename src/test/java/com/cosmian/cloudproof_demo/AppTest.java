package com.cosmian.cloudproof_demo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Paths;
import java.util.logging.Level;

import com.cosmian.rest.abe.acccess_policy.AccessPolicy;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class AppTest {

    @BeforeAll
    public static void before_all() {
        App.configureLog4j();
        App.initLogging(Level.FINER);
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
        File k = Paths.get(path, KeyGenerator.SSE_K_KEY_FILENAME).toFile();
        assertTrue(k.exists());
        assertEquals(32, k.length());
        File k_star = Paths.get(path, KeyGenerator.SSE_K_STAR_KEY_FILENAME).toFile();
        assertTrue(k_star.exists());
        assertEquals(32, k_star.length());
    }

    // @Test
    public void encryptDecrypt() throws Exception {

        String wd = Paths.get("").toAbsolutePath().toString();

        // encrypt
        String[] args_enc = new String[] { "-e", "-k", wd + "/src/test/resources/keys/public_key.json", "-o",
                wd + "/src/test/resources/enc/", wd + "/src/test/resources/users.txt" };
        App.run(args_enc);

        // search Alice
        String[] args_search_1 = new String[] { "--search", "-k", wd +
                "/src/test/resources/keys/user_Alice_key.json",
                "-o",
                wd + "/src/test/resources/dec/", "-c", "alice_search.txt", wd +
                        "/src/test/resources/enc",
                "country=france" };
        App.run(args_search_1);

        // search Bob
        String[] args_search_2 = new String[] { "--search", "-k", wd +
                "/src/test/resources/keys/user_Bob_key.json",
                "-o",
                wd + "/src/test/resources/dec/", "-c", "bob_search.txt", wd +
                        "/src/test/resources/enc",
                "country=spain" };
        App.run(args_search_2);

        // // search Alice
        // String[] args_dec_1 = new String[] { "--decrypt", "-k", wd +
        // "/src/test/resources/keys/user_Alice_key.json",
        // "-o",
        // wd + "/src/test/resources/dec/", "-c", "alice_decrypt.txt", wd +
        // "/src/test/resources/enc" };
        // App.run(args_dec_1);

        // // decrypt Bob
        // String[] args_dec_2 = new String[] { "--decrypt", "-k", wd +
        // "/src/test/resources/keys/user_Bob_key.json",
        // "-o",
        // wd + "/src/test/resources/dec/", "-c", "bob_decrypt.txt", wd +
        // "/src/test/resources/enc" };
        // App.run(args_dec_2);

    }

    @Test
    public void accessPolicyTest() throws Exception {
        String[] countries = new String[] { "France", "Germany", "Spain", "Belgium" };
        AccessPolicy ap = KeyGenerator.accessPolicy(new String[] { "marketing" }, countries);
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(ap);
        System.out.println(json);
    }

}
