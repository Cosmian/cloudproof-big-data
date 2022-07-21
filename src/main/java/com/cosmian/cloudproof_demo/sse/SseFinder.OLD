package com.cosmian.cloudproof_demo.sse;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.cosmian.CosmianException;
import com.cosmian.cloudproof_demo.AppException;
import com.cosmian.cloudproof_demo.DseDB;
import com.cosmian.cloudproof_demo.sse.Sse.DbUid;
import com.cosmian.cloudproof_demo.sse.Sse.Key;
import com.cosmian.cloudproof_demo.sse.Sse.Word;

public class SseFinder {

    private static final Logger logger = Logger.getLogger(SseFinder.class.getName());

    public static Set<byte[]> find(Set<Word> words, boolean disjunction, Key k, DseDB.Configuration dseConf)
        throws AppException {

        HashMap<Word, Set<DbUid>> result;
        try (DseDB dseDb = new DseDB(dseConf)) {
            try {
                result = Sse.bulkRetrieve(k, words, dseDb);
            } catch (CosmianException e) {
                throw new AppException("failed querying the SSE index: " + e.getMessage(), e);
            }
        } catch (CosmianException e) {
            throw new AppException("Failed initializing the SSE DB: " + e.getMessage(), e);
        }
        logger.fine(() -> "found results for " + result.size() + " word(s)");

        if (disjunction) {
            final Set<byte[]> set = new HashSet<byte[]>();
            for (Set<DbUid> s : result.values()) {
                set.addAll(s.stream().map(dbUid -> dbUid.bytes()).collect(Collectors.toSet()));
            }
            return set;
        }

        // conjunction
        Iterator<Set<DbUid>> all = result.values().iterator();
        if (all.hasNext()) {
            Set<DbUid> res = all.next();
            while (all.hasNext()) {
                Set<DbUid> previous = new HashSet<>();
                previous.addAll(res);
                res.clear();
                for (DbUid uid : all.next()) {
                    if (previous.contains(uid)) {
                        res.add(uid);
                    }
                }
            }
            return res.stream().map(dbUid -> dbUid.bytes()).collect(Collectors.toSet());
        }

        // no results
        return new HashSet<>();
    }

}
