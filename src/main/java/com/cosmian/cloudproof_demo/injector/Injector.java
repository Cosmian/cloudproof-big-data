package com.cosmian.cloudproof_demo.injector;

import java.util.List;

import com.cosmian.cloudproof_demo.AppException;
import com.cosmian.cloudproof_demo.DseDB;
import com.cosmian.cloudproof_demo.sse.Sse.Key;

public interface Injector {

    void run(Key k, Key kStar, String publicKeyJson, String outputDirectory, DseDB.Configuration dseConf,
        List<String> inputs) throws AppException;
}
