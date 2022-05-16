package com.cosmian.bnpp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Paths;

import com.cosmian.cloudproof_demo.App;
import com.cosmian.cloudproof_demo.KeyGenerator;
import com.cosmian.rest.abe.acccess_policy.AccessPolicy;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class AppTest {

    @BeforeAll
    public static void before_all() {
        TestUtils.initLogging();
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

    @Test
    public void encryptDecrypt() throws Exception {

        String wd = Paths.get("").toAbsolutePath().toString();

        // encrypt
        String[] args_enc = new String[] { "-e", "-k", wd + "/src/test/resources/keys/public_key.json", "-o",
                wd + "/src/test/resources/enc/", wd + "/src/test/resources/sample8.txt" };
        App.main(args_enc);

        // decrypt
        String[] args_dec_1 = new String[] { "-d", "-k", wd + "/src/test/resources/keys/user_BNPPF_France_key.json",
                "-o",
                wd + "/src/test/resources/dec/", "-c", "sample8.txt", wd + "/src/test/resources/enc" };
        App.main(args_dec_1);

        // decrypt
        String[] args_dec_2 = new String[] { "-d", "-k", wd + "/src/test/resources/keys/user_ALL_ALL_key.json", "-o",
                wd + "/src/test/resources/dec/", "-c", "sample.txt", wd + "/src/test/resources/enc" };
        App.main(args_dec_2);

    }

    @Test
    public void accessPolicyTest() throws Exception {
        String[] countries = new String[] { "France", "Germany", "Italy", "Hungary", "Spain", "Belgium" };
        AccessPolicy ap = KeyGenerator.accessPolicy(new String[] { "BNPPF" }, countries);
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(ap);
        System.out.println(json);
    }

}
