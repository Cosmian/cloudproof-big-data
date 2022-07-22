package com.cosmian.cloudproof_demo;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.lang.reflect.InvocationTargetException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.stream.Collectors;

import com.cosmian.cloudproof_demo.sse.DBEntryTableRecord;
import com.cosmian.cloudproof_demo.sse.Sse.Bytes;
import com.cosmian.cloudproof_demo.sse.Sse.Key;
import com.cosmian.cloudproof_demo.sse.Sse.WordHash;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class DseTest {

    @BeforeAll
    public static void before_all() {
        App.configureLog4j();
        App.initLogging(Level.FINE);
    }

    private <B extends Bytes> HashMap<B, byte[]> generateByteEntries(Random rd, Class<B> clazz, int numEntries,
        int ciphertextLen) {
        // generate NUM_LINES DB UIds of WORDS_PER_LINE words each
        System.out.print("Generating " + numEntries + " entries with key " + clazz.getName() + "....");
        final long genStart = System.nanoTime();
        HashMap<B, byte[]> entries = new HashMap<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            byte[] keyB = new byte[32];
            rd.nextBytes(keyB);
            B key;
            try {
                key = clazz.getDeclaredConstructor(byte[].class).newInstance(keyB);
            } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
                | InvocationTargetException | NoSuchMethodException | SecurityException e) {
                System.err.println("Could not generate entries: " + e.getMessage());
                e.printStackTrace();
                throw new RuntimeException("Could not generate entries: " + e.getMessage(), e);
            }
            byte[] ciphertext = new byte[ciphertextLen];
            rd.nextBytes(ciphertext);
            entries.put(key, ciphertext);
        }
        final long genTime = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - genStart);
        System.out.println(genTime + "µs i.e. " + (genTime / numEntries) + "µs/line");
        return entries;
    }

    private <B extends Bytes> Map<B, DBEntryTableRecord> generateEntryTableEntries(Random rd, Class<B> clazz,
        int numEntries, int ciphertextLen) {
        return generateByteEntries(rd, clazz, numEntries, ciphertextLen).entrySet().stream()
            .map(e -> new Map.Entry<B, DBEntryTableRecord>() {

                @Override
                public B getKey() {
                    return e.getKey();
                }

                @Override
                public DBEntryTableRecord getValue() {
                    return new DBEntryTableRecord() {

                        @Override
                        public int getRevision() {
                            return 0;
                        }

                        @Override
                        public byte[] getEncryptedValue() {
                            return e.getValue();
                        }

                    };
                }

                @Override
                public DBEntryTableRecord setValue(DBEntryTableRecord value) {

                    return null;
                }

            }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Test
    public void upsertRetrieve() throws Exception {

        final SecureRandom rd = new SecureRandom();
        try (DseDB db = new DseDB()) {
            final int NUM_ENTRIES = 10;

            // Entry Table
            db.truncateEntryTable();
            Map<WordHash, DBEntryTableRecord> entryTableEntries =
                generateEntryTableEntries(rd, WordHash.class, NUM_ENTRIES, 32);
            Map<WordHash, Boolean> firstInsertResults = db.upsertEntryTableEntries(entryTableEntries);
            assertEquals(NUM_ENTRIES, db.entryTableSize());
            assertEquals(NUM_ENTRIES, firstInsertResults.size());
            firstInsertResults.values().stream().forEach(b -> assertEquals(true, b));

            HashMap<WordHash, DBEntryTableRecord> entryResults = db.getEntryTableEntries(entryTableEntries.keySet());
            assertEquals(entryTableEntries.size(), entryResults.size());
            for (Map.Entry<WordHash, DBEntryTableRecord> entry : entryResults.entrySet()) {
                DBEntryTableRecord original = entryTableEntries.get(entry.getKey());
                assertNotNull(original);
                assertEquals(original.getRevision(), entry.getValue().getRevision());
                assertArrayEquals(original.getEncryptedValue(), entry.getValue().getEncryptedValue());
            }

            // re-insert the same values
            Map<WordHash, Boolean> secondInsertResults = db.upsertEntryTableEntries(entryTableEntries);
            assertEquals(NUM_ENTRIES, secondInsertResults.size());
            secondInsertResults.values().stream().forEach(b -> assertEquals(false, b));

            // update the revision of the first 5
            Map<WordHash, DBEntryTableRecord> updatedEntryTableEntries = new HashMap<>(entryTableEntries.size());
            entryTableEntries.entrySet().stream().forEach((entry) -> {
                updatedEntryTableEntries.put(entry.getKey(), new DBEntryTableRecord() {

                    @Override
                    public int getRevision() {
                        return entry.getValue().getRevision() + 1;
                    }

                    @Override
                    public byte[] getEncryptedValue() {
                        return entry.getValue().getEncryptedValue();
                    }

                });
            });
            Map<WordHash, Boolean> firstUpdateResults = db.upsertEntryTableEntries(updatedEntryTableEntries);
            assertEquals(NUM_ENTRIES, firstUpdateResults.size());
            firstUpdateResults.values().stream().forEach(b -> assertEquals(true, b));

            // second update - should return false with the same revisions ids
            Map<WordHash, Boolean> secondUpdateResults = db.upsertEntryTableEntries(updatedEntryTableEntries);
            assertEquals(NUM_ENTRIES, secondUpdateResults.size());
            secondUpdateResults.values().stream().forEach(b -> assertEquals(false, b));

            // Chain Table
            db.truncateChainTable();
            HashMap<Key, byte[]> chainTableEntries = generateByteEntries(rd, Key.class, NUM_ENTRIES, 32);
            db.upsertChainTableEntries(chainTableEntries);
            assertEquals(NUM_ENTRIES, db.chainTableSize());

            Set<byte[]> chainResults = db.getChainTableEntries(chainTableEntries.keySet());
            Collection<byte[]> originals = chainTableEntries.values();
            assertEquals(chainTableEntries.size(), chainResults.size());
            for (byte[] value : chainResults) {
                originals = originals.stream().filter(v -> !Arrays.equals(v, value)).collect(Collectors.toList());
            }
            assertEquals(0, originals.size());
        }
    }

}
