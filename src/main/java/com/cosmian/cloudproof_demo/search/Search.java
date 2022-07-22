package com.cosmian.cloudproof_demo.search;

import java.util.Set;

import com.cosmian.cloudproof_demo.AppException;
import com.cosmian.cloudproof_demo.DseDB;
import com.cosmian.cloudproof_demo.sse.Sse.Key;
import com.cosmian.cloudproof_demo.sse.Sse.Word;

public interface Search {

    public long[] run(Set<Word> words, boolean disjunction, String fsRootUri, Key k, String privateKeyJson,
        DseDB.Configuration dseConf, String outputDirectory, String cleartextFilename) throws AppException;
}
