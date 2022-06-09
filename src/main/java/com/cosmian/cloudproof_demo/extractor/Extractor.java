package com.cosmian.cloudproof_demo.extractor;

import java.util.List;

import com.cosmian.cloudproof_demo.AppException;

public interface Extractor {
    /**
     * Extract the entries that can be decrypted from the given list of inputs
     * 
     * @param inputs URIs / directories containing the encrypted files
     * @param privateKeyJson the private attributes key to use
     * @param outputDirectory the directory to output the clearTestFilename to
     * @param clearTextFilename the name of the file containing the decrypted records
     * @return the number of records decrypted
     * @throws AppException if anything unexpected happens
     */
    long run(List<String> inputs, String privateKeyJson, String outputDirectory, String clearTextFilename)
        throws AppException;
}
