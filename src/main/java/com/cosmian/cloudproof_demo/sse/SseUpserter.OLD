package com.cosmian.cloudproof_demo.sse;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Optional;
import java.util.Set;

import com.cosmian.CosmianException;
import com.cosmian.cloudproof_demo.AppException;
import com.cosmian.cloudproof_demo.Benchmarks;
import com.cosmian.cloudproof_demo.DseDB;
import com.cosmian.cloudproof_demo.sse.Sse.DbUid;
import com.cosmian.cloudproof_demo.sse.Sse.Key;
import com.cosmian.cloudproof_demo.sse.Sse.Word;

public class SseUpserter implements AutoCloseable, Serializable {

    // SSE Indexes-we want to accumulate some o that the servers does not learn
    // anything by running statistical analysis on what is inserted
    // So say we want to run insert batches of
    final int SSE_BATCH = 100;

    private final Key k;

    private final Key kStar;

    private final DseDB sseDb;

    private final Optional<Benchmarks> benchmarks;

    private final HashMap<DbUid, Set<Word>> dbUidToWords;

    public SseUpserter(Key k, Key kStar, DseDB.Configuration dseConf, Optional<Benchmarks> benchmarks)
        throws AppException {
        this.k = k;
        this.kStar = kStar;
        try {
            this.sseDb = new DseDB(dseConf);
        } catch (CosmianException e) {
            throw new AppException("Failed initializing the SSE DB: " + e.getMessage(), e);
        }
        this.benchmarks = benchmarks;
        this.dbUidToWords = new HashMap<>();
    }

    public void upsert(byte[] uid, Set<Word> words) throws AppException {

        dbUidToWords.put(new DbUid(uid), words);

        // the index values are upserted in a batch; this mixes data for the server and
        // provides additional statistical security
        if (dbUidToWords.size() == SSE_BATCH) {
            try {
                long[] times = Sse.bulkUpsert(this.k, this.kStar, dbUidToWords, this.sseDb);
                if (benchmarks.isPresent()) {
                    benchmarks.get().record("record_sse_crypto", SSE_BATCH, times[0]);
                    benchmarks.get().record("record_sse_db", SSE_BATCH, times[1]);
                }
            } catch (CosmianException e) {
                throw new AppException("Failed upserting the indexes: " + e.getMessage(), e);
            }
            dbUidToWords.clear();
        }

    }

    @Override
    public void close() throws AppException {
        // flush the remaining SSE indexes
        if (dbUidToWords.size() >= 0) {
            try {
                long[] times = Sse.bulkUpsert(this.k, this.kStar, dbUidToWords, this.sseDb);
                if (benchmarks.isPresent()) {
                    benchmarks.get().record("record_sse_crypto", dbUidToWords.size(), times[0]);
                    benchmarks.get().record("record_sse_db", dbUidToWords.size(), times[1]);
                }

            } catch (CosmianException e) {
                throw new AppException("Failed upserting the indexes: " + e.getMessage(), e);
            }
        }
        this.sseDb.close();
    }

    /**
     * Truncate the index
     * 
     * @param dseConf
     * @throws AppException
     */
    public static void truncate(DseDB.Configuration dseConf) throws AppException {
        try (DseDB sseDb = new DseDB(dseConf)) {
            sseDb.truncateEntryTable();
            sseDb.truncateChainTable();
        } catch (CosmianException e) {
            throw new AppException("Failed initializing the SSE DB: " + e.getMessage(), e);
        }
    }
}
