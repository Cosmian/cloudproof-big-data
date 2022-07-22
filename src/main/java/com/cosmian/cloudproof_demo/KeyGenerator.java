package com.cosmian.cloudproof_demo;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Optional;
import java.util.logging.Logger;

import com.cosmian.CosmianException;
import com.cosmian.RestClient;
import com.cosmian.cloudproof_demo.fs.OutputDirectory;
import com.cosmian.cloudproof_demo.policy.Country;
import com.cosmian.cloudproof_demo.policy.DemoPolicy;
import com.cosmian.cloudproof_demo.policy.Department;
import com.cosmian.rest.cover_crypt.CoverCrypt;
import com.cosmian.rest.cover_crypt.acccess_policy.AccessPolicy;
import com.cosmian.rest.cover_crypt.acccess_policy.And;
import com.cosmian.rest.cover_crypt.acccess_policy.Attr;
import com.cosmian.rest.cover_crypt.acccess_policy.Or;
import com.cosmian.rest.cover_crypt.policy.Policy;
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
        this.generateAsymmetricKeys();
        this.generateSymmetricKeys();
    }

    public void generateSymmetricKeys() throws AppException, CosmianException {
        SecureRandom rd = new SecureRandom();

        byte[] bytes = new byte[32];
        rd.nextBytes(bytes);
        try {
            this.outDirectory.getFs().writeFile(this.outDirectory.getDirectory().resolve(SSE_K_KEY_FILENAME).toString(),
                bytes);
        } catch (AppException e) {
            throw new AppException("Failed saving the SSE K key: " + e.getMessage(), e.getCause());
        }

        rd.nextBytes(bytes);
        try {
            this.outDirectory.getFs()
                .writeFile(this.outDirectory.getDirectory().resolve(SSE_K_STAR_KEY_FILENAME).toString(), bytes);
        } catch (AppException e) {
            throw new AppException("Failed saving the SSE K* key: " + e.getMessage(), e.getCause());
        }
    }

    public void generateAsymmetricKeys() throws AppException, CosmianException {

        CoverCrypt cc = new CoverCrypt(new RestClient(KeyGenerator.kmsServerUrl(), KeyGenerator.apiKey()));

        Policy policy = DemoPolicy.getInstance();

        String[] ids = cc.createMasterKeyPair(policy);
        logger.info("Created Master Key: Private Key ID: " + ids[0] + ", Public Key ID: " + ids[1]);

        String privateMasterKeyID = ids[0];
        PrivateKey privateMasterKey = cc.retrievePrivateMasterKey(privateMasterKeyID);
        try {
            this.outDirectory.getFs().writeFile(
                this.outDirectory.getDirectory().resolve("master_private_key.json").toString(),
                privateMasterKey.toJson().getBytes(StandardCharsets.UTF_8));
        } catch (AppException e) {
            throw new AppException("Failed saving the master private key: " + e.getMessage(), e.getCause());
        }

        String publicKeyID = ids[1];
        PublicKey publicKey = cc.retrievePublicMasterKey(publicKeyID);
        try {
            this.outDirectory.getFs().writeFile(this.outDirectory.getDirectory().resolve("public_key.json").toString(),
                publicKey.toJson().getBytes(StandardCharsets.UTF_8));
        } catch (AppException e) {
            throw new AppException("Failed saving the public key: " + e.getMessage(), e.getCause());
        }

        generateUserKey("Alice", cc, privateMasterKeyID,
            accessPolicy(new Department[] {Department.Marketing}, new Country[] {Country.France}));

        generateUserKey("Bob", cc, privateMasterKeyID,
            accessPolicy(new Department[] {Department.HR}, new Country[] {Country.Spain}));

        generateUserKey("Charlie", cc, privateMasterKeyID,
            accessPolicy(new Department[] {Department.HR}, new Country[] {Country.France}));

        generateUserKey("SuperAdmin", cc, privateMasterKeyID, accessPolicy(Department.values(), Country.values()));

        generateUserKey("Mallory", cc, privateMasterKeyID,
            accessPolicy(new Department[] {Department.Other}, new Country[] {Country.Other}));

    }

    /**
     * Generate an access policy (department_1 | department 2... | department_n) & (country_1 | country 2... |
     * country_n)
     * 
     * @param departments
     * @param regions
     * @return
     */
    public static AccessPolicy accessPolicy(Department[] departments, Country[] regions) throws CosmianException {

        AccessPolicy departmentsPolicy;
        if (departments.length == 0) {
            throw new CosmianException("The policy must have at least one department");
        } else if (departments.length == 1) {
            departmentsPolicy = new Attr(DemoPolicy.DEPARTMENT_AXIS, departments[0].toString());
        } else {
            departmentsPolicy = new Attr(DemoPolicy.DEPARTMENT_AXIS, departments[0].toString());
            for (int i = 1; i < departments.length; i++) {
                departmentsPolicy =
                    new Or(departmentsPolicy, new Attr(DemoPolicy.DEPARTMENT_AXIS, departments[i].toString()));
            }
        }

        AccessPolicy countriesPolicy;
        if (regions.length == 0) {
            throw new CosmianException("The policy must have at least one region");
        } else if (regions.length == 1) {
            countriesPolicy = new Attr(DemoPolicy.COUNTRY_AXIS, regions[0].toString());
        } else {
            countriesPolicy = new Attr(DemoPolicy.COUNTRY_AXIS, regions[0].toString());
            for (int i = 1; i < regions.length; i++) {
                countriesPolicy = new Or(countriesPolicy, new Attr(DemoPolicy.COUNTRY_AXIS, regions[i].toString()));
            }
        }

        return new And(departmentsPolicy, countriesPolicy);
    }

    void generateUserKey(String name, CoverCrypt cc, String privateMasterKeyID, AccessPolicy accessPolicy)
        throws CosmianException, AppException {
        String userDecryptionKeyID = cc.createUserDecryptionKey(accessPolicy, privateMasterKeyID);
        PrivateKey userDecryptionKey = cc.retrieveUserDecryptionKey(userDecryptionKeyID);
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

    public static Optional<String> apiKey() {
        String v = System.getenv("COSMIAN_API_KEY");
        if (v == null) {
            return Optional.of(
                "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IlRWdk5xTEtoUHhUSGdhYUNGRGRoSSJ9.eyJnaXZlbl9uYW1lIjoiTGFldGl0aWEiLCJmYW1pbHlfbmFtZSI6Ikxhbmdsb2lzIiwibmlja25hbWUiOiJsYWV0aXRpYS5sYW5nbG9pcyIsIm5hbWUiOiJMYWV0aXRpYSBMYW5nbG9pcyIsInBpY3R1cmUiOiJodHRwczovL2xoMy5nb29nbGV1c2VyY29udGVudC5jb20vYS9BQVRYQUp5UEJsSnpqRzNuMWZLLXNyS0ptdUVkYklUX29QRmhVbTd2T2dVWD1zOTYtYyIsImxvY2FsZSI6ImZyIiwidXBkYXRlZF9hdCI6IjIwMjEtMTItMjFUMDk6MjE6NDkuMDgxWiIsImVtYWlsIjoibGFldGl0aWEubGFuZ2xvaXNAY29zbWlhbi5jb20iLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwiaXNzIjoiaHR0cHM6Ly9kZXYtMW1ic2JtaW4udXMuYXV0aDAuY29tLyIsInN1YiI6Imdvb2dsZS1vYXV0aDJ8MTA4NTgwMDU3NDAwMjkxNDc5ODQyIiwiYXVkIjoiYUZqSzJvTnkwR1RnNWphV3JNYkJBZzV0bjRIV3VJN1ciLCJpYXQiOjE2NDAwNzg1MTQsImV4cCI6MTY0MDExNDUxNCwibm9uY2UiOiJha0poV2xoMlRsTm1lRTVtVFc0NFJHSk5VVEl5WW14aVJUTnVRblV1VEVwa2RrTnFVa2R5WkdoWFdnPT0ifQ.Q4tCzvJTNxmDhIYOJbjsqupdQkWg29Ny0B8njEfSrLVXNaRMFE99eSXedCBaXSMBnZ9GuCV2Z1MAZL8ZjTxqPP_VYCnc2QufG1k1XZg--6Q48pPdpUBXu2Ny1eatwiDrRvgQfUHkiM8thUAOb4bXxGLrtQKlO_ePOehDbEOjfd11aVm3pwyVqj1v6Ki1D5QJsOHtkkpLMinmmyGDtmdHH2YXseZNHGUY7PWZ6DelpJaxI48W5FNDY4b0sJlzaJqdIcoOX7EeP1pfFoHVeZAo5mWyuDev2OaPYKeqpga4PjqHcFT0m1rQoWQHmfGr3EkA3w8NXmKnZmEbQcLLgcCATw");
            // return Optional.empty();
        }
        return Optional.of(v);
    }

}
