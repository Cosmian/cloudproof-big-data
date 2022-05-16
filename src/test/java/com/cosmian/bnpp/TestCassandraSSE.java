package com.cosmian.bnpp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;

import com.cosmian.CosmianException;
import com.cosmian.cloudproof_demo.App;
import com.cosmian.cloudproof_demo.DseDB;
import com.cosmian.cloudproof_demo.sse.Sse;
import com.cosmian.cloudproof_demo.sse.Sse.DbUid;
import com.cosmian.cloudproof_demo.sse.Sse.Key;
import com.cosmian.cloudproof_demo.sse.Sse.Word;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestCassandraSSE {

	@BeforeAll
	public static void before_all() {
		App.configureLog4j();
		App.initLogging(Level.INFO);
	}

	@Test
	public void testUpsertRetrieve() throws Exception {

		DseDB db;
		try {
			db = new DseDB();
		} catch (CosmianException e) {
			System.out.println("No local DSE db available. Skipping test.\n\n" + e.getMessage());
			return;
		}
		db.truncateChainTable();
		db.truncateEntryTable();

		Random rd = new Random();
		Key k = new Key(rd);
		Key kStar = new Key(rd);

		DbUid dbUid1 = new DbUid(rd, 47);
		Word word0 = new Word(rd, 32);
		Word word11 = new Word(rd, 29);
		Word word12 = new Word(rd, 243);

		DbUid dbUid2 = new DbUid(rd, 728);
		Word word21 = new Word(rd, 5);
		Word word22 = new Word(rd, 73);

		HashMap<DbUid, Set<Word>> dbUidToWords = new HashMap<>();
		Set<Word> words1 = new HashSet<>();
		words1.add(word0);
		words1.add(word11);
		words1.add(word12);
		dbUidToWords.put(dbUid1, words1);
		Set<Word> words2 = new HashSet<>();
		words2.add(word0);
		words2.add(word21);
		words2.add(word22);
		words2.add(word21);
		dbUidToWords.put(dbUid2, words2);

		Set<Word> set2 = dbUidToWords.get(dbUid2);
		assertEquals(3, set2.size());

		Sse.bulkUpsert(k, kStar, dbUidToWords, db);
		assertEquals(5, db.entryTableSize());
		assertEquals(6, db.chainTableSize());

		// Find word0
		Set<Word> words = new HashSet<>();
		words.add(word0);
		HashMap<Word, Set<DbUid>> result = Sse.bulkRetrieve(k, words, db);
		assertEquals(1, result.size());
		Set<DbUid> dbUidSet = result.get(word0);
		assertNotNull(dbUidSet);
		assertEquals(2, dbUidSet.size());
		assertTrue(dbUidSet.contains(dbUid1));
		assertTrue(dbUidSet.contains(dbUid2));

		// Find word11
		words = new HashSet<>();
		words.add(word11);
		result = Sse.bulkRetrieve(k, words, db);
		assertEquals(1, result.size());
		dbUidSet = result.get(word11);
		assertNotNull(dbUidSet);
		assertEquals(1, dbUidSet.size());
		assertTrue(dbUidSet.contains(dbUid1));

		// Find word22
		words = new HashSet<>();
		words.add(word22);
		result = Sse.bulkRetrieve(k, words, db);
		assertEquals(1, result.size());
		dbUidSet = result.get(word22);
		assertNotNull(dbUidSet);
		assertEquals(1, dbUidSet.size());
		assertTrue(dbUidSet.contains(dbUid2));

		// Find word0, word11, word22
		words = new HashSet<>();
		words.add(word0);
		words.add(word11);
		words.add(word22);
		result = Sse.bulkRetrieve(k, words, db);
		assertEquals(3, result.size());
		// word 0
		dbUidSet = result.get(word0);
		assertNotNull(dbUidSet);
		assertEquals(2, dbUidSet.size());
		assertTrue(dbUidSet.contains(dbUid1));
		assertTrue(dbUidSet.contains(dbUid2));
		// word11
		dbUidSet = result.get(word11);
		assertNotNull(dbUidSet);
		assertEquals(1, dbUidSet.size());
		assertTrue(dbUidSet.contains(dbUid1));
		// word22
		dbUidSet = result.get(word22);
		assertNotNull(dbUidSet);
		assertEquals(1, dbUidSet.size());
		assertTrue(dbUidSet.contains(dbUid2));

	}

}
