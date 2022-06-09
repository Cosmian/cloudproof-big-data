package com.cosmian.cloudproof_demo.sse;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.AlgorithmParameterSpec;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.Mac;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import com.cosmian.CosmianException;

public class Sse {

    private final static Logger logger = Logger.getLogger(Sse.class.getName());

    private final static SecureRandom SECURE_RANDOM = new SecureRandom();

    private static byte[] encryptAes(SecureRandom rd, Key key, byte[] plaintext) throws CosmianException {
        try {
            SecretKey secretKey = new SecretKeySpec(key.bytes(), "AES");
            byte[] nonce = new byte[12];
            rd.nextBytes(nonce);
            final Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            GCMParameterSpec parameterSpec = new GCMParameterSpec(16 * 8, nonce); // 128 bit auth tag length
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, parameterSpec);
            byte[] ciphertext = cipher.doFinal(plaintext);
            byte[] result = new byte[12 + ciphertext.length];
            System.arraycopy(nonce, 0, result, 0, 12);
            System.arraycopy(ciphertext, 0, result, 12, ciphertext.length);
            return result;
        } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException
            | InvalidAlgorithmParameterException | IllegalBlockSizeException | BadPaddingException e) {
            throw new CosmianException("Failed encrypting: " + e.getMessage(), e);
        }
    }

    private static byte[] decryptAes(SecureRandom rd, Key key, byte[] ciphertext) throws CosmianException {
        try {
            SecretKey secretKey = new SecretKeySpec(key.bytes(), "AES");
            byte[] nonce = new byte[12];
            rd.nextBytes(nonce);
            final Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            // use first 12 bytes for iv
            AlgorithmParameterSpec gcmIv = new GCMParameterSpec(16 * 8, ciphertext, 0, 12);
            cipher.init(Cipher.DECRYPT_MODE, secretKey, gcmIv);
            return cipher.doFinal(ciphertext, 12, ciphertext.length - 12);
        } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException
            | InvalidAlgorithmParameterException | IllegalBlockSizeException | BadPaddingException e) {
            throw new CosmianException("Failed decrypting: " + e.getMessage(), e);
        }
    }

    /**
     * A Byte array that implements hashcode() and equals() and hence can be used in Maps
     */
    public static abstract class Bytes implements Serializable {
        protected final byte[] bytes;

        /**
         * Wrap anx existing array
         * 
         * @param bytes the existing bytes
         */
        public Bytes(byte[] bytes) {
            this.bytes = bytes;
        }

        /**
         * Generate a random byte array
         * 
         * @param rd the {@link Random} generator
         * @param length the byte array length
         */
        public Bytes(Random rd, int length) {
            this.bytes = new byte[length];
            rd.nextBytes(this.bytes);
        }

        public byte[] bytes() {
            return this.bytes;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Bytes word = (Bytes) o;
            return Arrays.equals(bytes, word.bytes);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }

        @Override
        public String toString() {
            return Base64.getEncoder().encodeToString(this.bytes);
        }

    }

    public static class Word extends Bytes {

        public Word(byte[] bytes) {
            super(bytes);
        }

        public Word(Random rd, int length) {
            super(rd, length);
        }

        public WordHash hash(byte[] salt) throws CosmianException {
            try {
                Mac sha256_HMAC = Mac.getInstance("HmacSHA256");
                SecretKeySpec secret_key = new SecretKeySpec(salt, "HmacSHA256");
                sha256_HMAC.init(secret_key);
                return new WordHash(sha256_HMAC.doFinal(this.bytes));
            } catch (NoSuchAlgorithmException | InvalidKeyException e) {
                throw new CosmianException("failed computing the word hash: " + e.getMessage(), e);
            }
        }
    }

    public static class WordHash extends Bytes {

        public WordHash(byte[] bytes) {
            super(bytes);
        }

        public WordHash(Random rd, int length) {
            super(rd, length);
        }

        public static WordHash fromString(String base64) {
            return new WordHash(Base64.getDecoder().decode(base64));
        }
    }

    public static class DbUid extends Bytes {

        public DbUid(byte[] bytes) {
            super(bytes);
        }

        public DbUid(Random rd, int length) {
            super(rd, length);
        }

        @Override
        public boolean equals(Object o) {

            if (o != null && o instanceof DbUid) {
                DbUid other = (DbUid) o;
                return Arrays.equals(this.bytes, other.bytes);
            }
            return false;
        }
    }

    /**
     * A Key is a byte array with a fixed size set by {@value Key#KEY_LENGTH}
     */
    public static class Key extends Bytes {

        // keys have a fixed 32 byte length
        public final static int KEY_LENGTH = 32;

        public Key(byte[] bytes) {
            super(Arrays.copyOf(bytes, KEY_LENGTH));
        }

        public Key(Random rd) {
            super(rd, KEY_LENGTH);
        }

        /**
         * Build a key by deriving another key and some derivation data
         * 
         * @param fromKey the other key bytes
         * @param data the derivation data
         * @throws CosmianException
         */
        public static Key derive(byte[] fromKey, byte[] data) throws CosmianException {
            try {
                Mac sha256_HMAC = Mac.getInstance("HmacSHA256");
                SecretKeySpec secret_key = new SecretKeySpec(fromKey, "HmacSHA256");
                sha256_HMAC.init(secret_key);
                return new Key(sha256_HMAC.doFinal(data));
            } catch (NoSuchAlgorithmException | InvalidKeyException e) {
                throw new CosmianException("failed deriving a new key: " + e.getMessage(), e);
            }
        }

        /**
         * Build a key by deriving another key and some derivation data
         * 
         * @param fromKey the key to derive from
         * @param data the derivation data
         * @throws CosmianException
         */
        public static Key derive(Key fromKey, byte[] data) throws CosmianException {
            return Key.derive(fromKey.bytes, data);
        }

    }

    /**
     * A clear text Entry table value. where - r is the last value of the chain and - Kwᵢ the key for keyword wᵢ which
     * is derived ah H(K*, wᵢ) where ---- K* is key only known by the Directory Authority and ---- wᵢ is the word which
     * is indexed The value saved in the table is the cipher text: AES(K₂, (r, Kwᵢ))
     */
    static class EntryTableValue {

        private Key r;

        private final Key kwi;

        public EntryTableValue(Key r, Key kStar, Word wi) throws CosmianException {
            this.r = r;
            this.kwi = Key.derive(kStar, wi.bytes());

        }

        protected EntryTableValue(Key r, Key kwi) throws CosmianException {
            this.r = r;
            this.kwi = kwi;
        }

        /**
         * Set r = H(Kwᵢ,r) which determines the next value in the chain to use in the Chain table
         */
        public void nextR() throws CosmianException {
            try {
                this.r = Key.derive(this.kwi, this.r.bytes());
            } catch (CosmianException e) {
                throw new CosmianException("failed generating the next r value: " + e.getMessage(), e);
            }
        }

        /**
         * The AES cipher text of the entry table value encrypted under key K₂
         * 
         * @param keyK2 the encryption key
         * @return the cipher text
         * @throws CosmianException
         */
        public byte[] toBytes(Key keyK2) throws CosmianException {
            byte[] plaintext = new byte[2 * Key.KEY_LENGTH];
            System.arraycopy(this.r.bytes, 0, plaintext, 0, Key.KEY_LENGTH);
            System.arraycopy(this.kwi.bytes, 0, plaintext, Key.KEY_LENGTH, Key.KEY_LENGTH);
            try {
                return encryptAes(SECURE_RANDOM, keyK2, plaintext);
            } catch (CosmianException e) {
                throw new CosmianException(
                    "Failed encrypting the Entry Table Value under key K₂: " + e.getCause().getMessage(), e);
            }
        }

        /**
         * Decrypt the Entry Table value using key K₂
         * 
         * @param encryptedValue the encrypted entry valu
         * @param keyK2 the decryption Key
         * @return a {@link EntryTableValue}
         * @throws CosmianException if the it cannot be decrypted
         */
        public static EntryTableValue fromBytes(byte[] encryptedValue, Key keyK2) throws CosmianException {
            byte[] plaintext;
            try {
                plaintext = decryptAes(SECURE_RANDOM, keyK2, encryptedValue);
            } catch (CosmianException e) {
                throw new CosmianException(
                    "Failed decrypting the Entry Table Value under key K₂: " + e.getCause().getMessage(), e);
            }
            Key r = new Key(Arrays.copyOfRange(plaintext, 0, Key.KEY_LENGTH));
            Key kwi = new Key(Arrays.copyOfRange(plaintext, Key.KEY_LENGTH, Key.KEY_LENGTH + Key.KEY_LENGTH));
            return new EntryTableValue(r, kwi);
        }
    }

    // These arbitrary salts are used to derive the Key K which is the symmetric key
    // known by every authorized clients
    // (= applications) - they can be overwritten by setting the public static
    // variable
    private static byte[] K1_SALT =
        {(byte) 0x6B, (byte) 0x66, (byte) 0xF3, (byte) 0x19, (byte) 0x28, (byte) 0x72, (byte) 0xFC, (byte) 0x41,
            (byte) 0xED, (byte) 0x17, (byte) 0x59, (byte) 0x74, (byte) 0x35, (byte) 0xAD, (byte) 0xE6, (byte) 0x62,
            (byte) 0xF0, (byte) 0x3D, (byte) 0x4A, (byte) 0x9F, (byte) 0x53, (byte) 0x6B, (byte) 0x76, (byte) 0xF2,
            (byte) 0x4E, (byte) 0xA9, (byte) 0xA2, (byte) 0xB0, (byte) 0xB4, (byte) 0xE6, (byte) 0x46, (byte) 0x57,
            (byte) 0x6A, (byte) 0xDA, (byte) 0x18, (byte) 0x04, (byte) 0x1B, (byte) 0x13, (byte) 0x6B, (byte) 0x5D,
            (byte) 0x9F, (byte) 0xD0, (byte) 0x1D, (byte) 0x20, (byte) 0x22, (byte) 0xB2, (byte) 0x85, (byte) 0x1F};

    private static byte[] K2_SALT =
        {(byte) 0x22, (byte) 0x8D, (byte) 0x81, (byte) 0xDE, (byte) 0x62, (byte) 0x04, (byte) 0xA6, (byte) 0xB4,
            (byte) 0x0E, (byte) 0xC5, (byte) 0xA9, (byte) 0x99, (byte) 0x11, (byte) 0x50, (byte) 0x6A, (byte) 0xFC,
            (byte) 0x38, (byte) 0xEE, (byte) 0x52, (byte) 0xDE, (byte) 0x97, (byte) 0xB7, (byte) 0x9C, (byte) 0xFC,
            (byte) 0x0F, (byte) 0x8E, (byte) 0x58, (byte) 0x06, (byte) 0xF7, (byte) 0xED, (byte) 0x48, (byte) 0x9E,
            (byte) 0xBF, (byte) 0x88, (byte) 0x55, (byte) 0x60, (byte) 0xD2, (byte) 0xAD, (byte) 0x5D, (byte) 0x09,
            (byte) 0xD0, (byte) 0x59, (byte) 0x19, (byte) 0x79, (byte) 0x52, (byte) 0x8E, (byte) 0x86, (byte) 0x55};

    private static int LOOP_ITERATION_LIMIT = 10000;

    /**
     * Upsert the a set of words for a list od DB UIDs The size of the map should be significant so that t is hard for
     * te server to learn anything from the operation by performing a simple statistical analysis
     * 
     * @param k the main symmetric key
     * @param kStar the symmetric key known to the updater only
     * @param dbUidToWords the set of words to index for each DB entry
     * @param db the db that holds the index
     * @throws CosmianException if anything wrong happens
     */
    public static long[] bulkUpsert(Key k, Key kStar, HashMap<DbUid, Set<Word>> dbUidToWords, DBInterface db)
        throws CosmianException {

        long cryptoTime = 0;
        long dbTime = 0;

        long thenCrypto1 = System.nanoTime();

        // First compute derived keys K1 and K2
        Key k1 = Key.derive(k, K1_SALT);
        Key k2 = Key.derive(k, K2_SALT);

        // build a map of clear text words to DbUidSet and a map of word hash to word
        HashMap<Word, Set<DbUid>> wordToDbUidSet = new HashMap<>();
        HashMap<WordHash, Word> wordHashToWord = new HashMap<>();
        for (Map.Entry<DbUid, Set<Word>> entry : dbUidToWords.entrySet()) {
            DbUid dbUid = entry.getKey();
            Set<Word> words = entry.getValue();
            for (Word wi : words) {
                Set<DbUid> dbUidSet = wordToDbUidSet.get(wi);
                if (dbUidSet == null) {
                    dbUidSet = new HashSet<>();
                }
                dbUidSet.add(dbUid);
                wordToDbUidSet.put(wi, dbUidSet);
                // the Entry table uses the hash of the word computed as H(K₁, wᵢ) as key
                wordHashToWord.put(wi.hash(k1.bytes), wi);
            }
        }

        // record time
        cryptoTime += TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - thenCrypto1);
        long thenDB1 = System.nanoTime();

        // a map of Table Entry, word hash to clear text values
        HashMap<WordHash, EntryTableValue> entryTableValues = new HashMap<>();
        // fetch the current values from the entry table the given words and decrypt
        // them
        for (Map.Entry<WordHash, byte[]> entry : db.getEntryTableEntries(wordHashToWord.keySet()).entrySet()) {
            entryTableValues.put(entry.getKey(), EntryTableValue.fromBytes(entry.getValue(), k2));
        }
        dbTime += TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - thenDB1);
        long thenCrypto2 = System.nanoTime();

        // the entries that will be updated in the Entry table and their encrypted
        // values
        HashMap<WordHash, byte[]> entryTableUpdates = new HashMap<>(wordHashToWord.size());

        // entries in the Chain Table
        HashMap<Key, byte[]> chainTableUpdates = new HashMap<>();

        for (Map.Entry<WordHash, Word> entry : wordHashToWord.entrySet()) {

            WordHash wordHash = entry.getKey();
            Word wi = entry.getValue();
            Set<DbUid> dbUidSet = wordToDbUidSet.get(wi);

            // check if we already updated that entry
            EntryTableValue entryTableValue = entryTableValues.get(wordHash);
            if (entryTableValue == null) {
                // Kwᵢ = H(K*, wᵢ)
                Key kwi = Key.derive(kStar, wi.bytes());
                // the start of the chan value is r = H(Kwᵢ, wᵢ)
                Key r = Key.derive(kwi, wi.bytes());
                entryTableValue = new EntryTableValue(r, kStar, wi);
            } else {
                // Increment the next r value to add an entry = AES(Kwᵢ, dbUID)
                // this value is the the key to the chain table
                entryTableValue.nextR();
            }

            // update the chain table
            Iterator<DbUid> it = dbUidSet.iterator();
            while (true) {
                DbUid dbUid = it.next();
                chainTableUpdates.put(entryTableValue.r, generateChainTableValue(entryTableValue.kwi, dbUid));
                if (it.hasNext()) {
                    // increment the next r value
                    entryTableValue.nextR();
                } else {
                    // done
                    break;
                }
            }

            // update the entry table
            entryTableUpdates.put(wordHash, entryTableValue.toBytes(k2));
        }

        cryptoTime += TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - thenCrypto2);
        long thenDB2 = System.nanoTime();

        // perform the DB updates
        db.upsertEntryTableEntries(entryTableUpdates);
        db.upsertChainTableEntries(chainTableUpdates);

        dbTime += TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - thenDB2);

        return new long[] {cryptoTime, dbTime};
    }

    /**
     * Generate the chain table value AES(Kwᵢ, dbUID)
     * 
     * @param kwi the derived key for the word
     * @param dbUid the DB table UUID
     * @return the generated value
     * @throws CosmianException if encryption fails
     */
    private static byte[] generateChainTableValue(Key kwi, DbUid dbUid) throws CosmianException {
        try {
            return encryptAes(SECURE_RANDOM, kwi, dbUid.bytes);
        } catch (CosmianException e) {
            throw new CosmianException(
                "Failed encrypting the Chain Table Value under key Kwᵢ: " + e.getCause().getMessage(), e);
        }
    }

    /**
     * Decrypt the Chain Table value to retrieve the UID
     * 
     * @param kwi the derived key for the word
     * @param encryptedValue the encrypted DB UID
     * @return the DB UID
     * @throws CosmianException if the value cannot be decrypted
     */
    public static DbUid getDbUidFromChainTableValue(Key kwi, byte[] encryptedValue) throws CosmianException {
        byte[] dbUid;
        try {
            dbUid = decryptAes(SECURE_RANDOM, kwi, encryptedValue);
        } catch (CosmianException e) {
            throw new CosmianException(
                "Failed decrypting the Chain Table Value under key Kwᵢ: " + e.getCause().getMessage(), e);
        }
        return new DbUid(dbUid);
    }

    /**
     * Retrieve the set of DB Uid for a given set of words
     * 
     * @param k the main symmetric key
     * @param words the set of words
     * @param db the key value store holding the indexes
     * @return a map of Word -> [dbUid,...]
     * @throws CosmianException if an error occurs
     */
    public static HashMap<Word, Set<DbUid>> bulkRetrieve(Key k, Set<Word> words, DBInterface db)
        throws CosmianException {

        // First compute derived keys K1 and K2
        Key k1 = Key.derive(k, K1_SALT);
        Key k2 = Key.derive(k, K2_SALT);

        HashMap<WordHash, Word> wordHashToWord = new HashMap<>();
        for (Word wi : words) {
            wordHashToWord.put(wi.hash(k1.bytes), wi);
        }

        HashMap<Word, Set<DbUid>> results = new HashMap<>();

        // retrieve the Entry Table entries for the words
        HashMap<WordHash, byte[]> entryTable = db.getEntryTableEntries(wordHashToWord.keySet());
        if (entryTable.size() == 0) {
            logger.fine(() -> "Search: words not found in the entry table");
            return results;
        }
        logger.fine(() -> "Search: found " + entryTable.size() + " words in entry table out of " + words.size()
            + " words searched");

        for (Map.Entry<WordHash, Word> entry : wordHashToWord.entrySet()) {
            WordHash wordHash = entry.getKey();
            Word wi = entry.getValue();
            byte[] encEntryTableValues = entryTable.get(wordHash);
            if (encEntryTableValues == null) {
                results.put(wi, new HashSet<>());
                continue;
            }
            EntryTableValue entryTableValue = EntryTableValue.fromBytes(encEntryTableValues, k2);

            Set<Key> chainTableKeys = new HashSet<>();
            // the start of the chan value is r = H(Kwᵢ, wᵢ)
            Key currentR = Key.derive(entryTableValue.kwi, wi.bytes);
            int i = 0;
            while (i < LOOP_ITERATION_LIMIT) {
                chainTableKeys.add(currentR);
                if (currentR.equals(entryTableValue.r)) {
                    break;
                }
                // get the next value in chain
                currentR = Key.derive(entryTableValue.kwi, currentR.bytes);
                i++;
            }
            Set<byte[]> encDbUidSet = db.getChainTableEntries(chainTableKeys);
            Set<DbUid> dbUidSet = new HashSet<>();
            for (byte[] encDbUid : encDbUidSet) {
                dbUidSet.add(getDbUidFromChainTableValue(entryTableValue.kwi, encDbUid));
            }
            logger.fine(() -> "Word " + new String(wi.bytes, StandardCharsets.UTF_8) + " has " + dbUidSet.size()
                + " distinct DB UIDs");
            results.put(wi, dbUidSet);
        }
        return results;
    }

}
