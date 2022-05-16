package com.cosmian.cloudproof_demo;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Optional;
import java.util.logging.Logger;

import com.cosmian.CosmianException;
import com.cosmian.RestClient;
import com.cosmian.cloudproof_demo.fs.OutputDirectory;
import com.cosmian.rest.abe.Abe;
import com.cosmian.rest.abe.acccess_policy.AccessPolicy;
import com.cosmian.rest.abe.acccess_policy.And;
import com.cosmian.rest.abe.acccess_policy.Attr;
import com.cosmian.rest.abe.acccess_policy.Or;
import com.cosmian.rest.abe.policy.Policy;
import com.cosmian.rest.kmip.objects.PrivateKey;
import com.cosmian.rest.kmip.objects.PublicKey;

public class KeyGenerator {

    public final static String SSE_K_KEY_FILENAME = "sse_k.key";
    public final static String SSE_K_STAR_KEY_FILENAME = "sse_k_star.key";

    private static final Logger logger = Logger.getLogger(KeyGenerator.class.getName());

    OutputDirectory outDirectory;

    public KeyGenerator(String outDirectory) throws AppException {
        this.outDirectory = OutputDirectory.parse(outDirectory);
    }

    public void generateKeys() throws AppException, CosmianException {
        this.generateABEKeys();
        this.generateSymmetricKeys();
    }

    public void generateSymmetricKeys() throws AppException, CosmianException {
        SecureRandom rd = new SecureRandom();

        byte[] bytes = new byte[32];
        rd.nextBytes(bytes);
        try {
            this.outDirectory.getFs().writeFile(
                    this.outDirectory.getDirectory().resolve(
                            SSE_K_KEY_FILENAME).toString(),
                    bytes);
        } catch (AppException e) {
            throw new AppException("Failed saving the SSE K key: " + e.getMessage(),
                    e.getCause());
        }

        rd.nextBytes(bytes);
        try {
            this.outDirectory.getFs().writeFile(
                    this.outDirectory.getDirectory().resolve(
                            SSE_K_STAR_KEY_FILENAME).toString(),
                    bytes);
        } catch (AppException e) {
            throw new AppException("Failed saving the SSE K* key: " + e.getMessage(),
                    e.getCause());
        }
    }

    public void generateABEKeys() throws AppException, CosmianException {

        Abe abe = new Abe(new RestClient(KeyGenerator.kmsServerUrl(), KeyGenerator.apiKey()));

        String[] entities = new String[] { "BCEF", "BNPPF", "CIB", "CashMgt" };
        String[] countries = new String[] { "France", "Germany", "Italy", "Hungary", "Spain", "Belgium" };

        Policy policy = new Policy(100)
                .addAxis("Entity", entities, false)
                .addAxis("Country", countries, false);

        String[] ids = abe.createMasterKeyPair(policy);
        logger.info("Created Master Key: Private Key ID: " + ids[0] + ", Public Key ID: " + ids[1]);

        String privateMasterKeyID = ids[0];
        PrivateKey privateMasterKey = abe.retrievePrivateMasterKey(privateMasterKeyID);
        try {
            this.outDirectory.getFs().writeFile(
                    this.outDirectory.getDirectory().resolve("master_private_key.json").toString(),
                    privateMasterKey.toJson().getBytes(StandardCharsets.UTF_8));
        } catch (AppException e) {
            throw new AppException("Failed saving the master private key: " + e.getMessage(),
                    e.getCause());
        }

        String publicKeyID = ids[1];
        PublicKey publicKey = abe.retrievePublicMasterKey(publicKeyID);
        try {
            this.outDirectory.getFs().writeFile(this.outDirectory.getDirectory().resolve("public_key.json").toString(),
                    publicKey.toJson().getBytes(StandardCharsets.UTF_8));
        } catch (AppException e) {
            throw new AppException("Failed saving the public key: " + e.getMessage(),
                    e.getCause());
        }

        generateUserKey("BNPPF_France", abe, privateMasterKeyID,
                accessPolicy(new String[] { "BNPPF" }, new String[] { "France" }));

        generateUserKey("BNPPF_Italy", abe, privateMasterKeyID,
                accessPolicy(new String[] { "BNPPF" }, new String[] { "Italy" }));

        generateUserKey("BCEF_France", abe, privateMasterKeyID,
                accessPolicy(new String[] { "BCEF" }, new String[] { "France" }));

        generateUserKey("CIB_Belgium", abe, privateMasterKeyID,
                accessPolicy(new String[] { "BCEF" }, new String[] { "Belgium" }));

        generateUserKey("BNPPF_ALL", abe, privateMasterKeyID, accessPolicy(new String[] { "BNPPF" }, countries));

        generateUserKey("ALL_France", abe, privateMasterKeyID,
                accessPolicy(entities, new String[] { "France" }));

        generateUserKey("ALL_ALL", abe, privateMasterKeyID,
                accessPolicy(entities, countries));

    }

    /**
     * Generate an access policy
     * (entity_1 | entity 2... | entity_n) & (country_1 | country 2... | country_n)
     * 
     * @param entities
     * @param countries
     * @return
     */
    public static AccessPolicy accessPolicy(String[] entities, String[] countries) throws CosmianException {

        AccessPolicy entitiesPolicy;
        if (entities.length == 0) {
            throw new CosmianException("The policy must have at least one entity");
        } else if (entities.length == 1) {
            entitiesPolicy = new Attr("Entity", entities[0]);
        } else {
            entitiesPolicy = new Attr("Entity", entities[0]);
            for (int i = 1; i < entities.length; i++) {
                entitiesPolicy = new Or(entitiesPolicy, new Attr("Entity", entities[i]));
            }
        }

        AccessPolicy countriesPolicy;
        if (countries.length == 0) {
            throw new CosmianException("The policy must have at least one entity");
        } else if (countries.length == 1) {
            countriesPolicy = new Attr("Country", countries[0]);
        } else {
            countriesPolicy = new Attr("Country", countries[0]);
            for (int i = 1; i < countries.length; i++) {
                countriesPolicy = new Or(countriesPolicy, new Attr("Country", countries[i]));
            }
        }

        return new And(entitiesPolicy, countriesPolicy);
    }

    void generateUserKey(String name, Abe abe, String privateMasterKeyID, AccessPolicy accessPolicy)
            throws CosmianException, AppException {
        String userDecryptionKeyID = abe.createUserDecryptionKey(accessPolicy, privateMasterKeyID);
        PrivateKey userDecryptionKey = abe.retrieveUserDecryptionKey(userDecryptionKeyID);
        String keyFile = "user_" + name + "_key.json";
        try {
            this.outDirectory.getFs().writeFile(this.outDirectory.getDirectory().resolve(keyFile).toString(),
                    userDecryptionKey.toJson().getBytes(StandardCharsets.UTF_8));
        } catch (AppException e) {
            throw new AppException("Failed saving the user decryption key: " + keyFile + ": " + e.getMessage(),
                    e.getCause());
        }
    }

    static String kmsServerUrl() {
        String v = System.getenv("COSMIAN_SERVER_URL");
        if (v == null) {
            return "http://localhost:9998";
        }
        return v;
    }

    static Optional<String> apiKey() {
        String v = System.getenv("COSMIAN_API_KEY");
        if (v == null) {
            return Optional.empty();
        }
        return Optional.of(v);
    }

}
