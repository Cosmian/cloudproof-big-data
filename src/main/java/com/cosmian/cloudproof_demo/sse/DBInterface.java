package com.cosmian.cloudproof_demo.sse;

import java.util.HashMap;
import java.util.Set;

import com.cosmian.CosmianException;
import com.cosmian.cloudproof_demo.sse.Sse.Key;
import com.cosmian.cloudproof_demo.sse.Sse.WordHash;

public interface DBInterface {

    /**
     * Retrieve the encrypted values of the Entry Table for a given set of word
     * hashes
     * 
     * @param wordHashes as set of word hashes (sated by K₁)
     * @return the entries of word hashes to encrypted values
     * @throws CosmianException if the map cannot be fetched
     */
    HashMap<WordHash, byte[]> getEntryTableEntries(Set<WordHash> wordHashes) throws CosmianException;

    /**
     * Upsert the entries (Word hash -> encrypted value) in the Entry Table
     * 
     * @param entries the entries to upsert
     * @throws CosmianException if the entries cannot be upserted
     */
    void upsertEntryTableEntries(HashMap<WordHash, byte[]> entries) throws CosmianException;

    /**
     * Retrieve the encrypted db UIDs from the Chain Table
     * 
     * @param chainTableKeys a list of chain table keys
     * @return the set of encrypted DB Uids
     * @throws CosmianException if the entries cannot be fetched
     */
    Set<byte[]> getChainTableEntries(Set<Key> chainTableKeys) throws CosmianException;

    /**
     * Upsert the entries (chain table keys -> encrypted DB uids) in the Entry Table
     * 
     * @param entries the entries to upsert
     * @throws CosmianException if the entries cannot be upserted
     */
    void upsertChainTableEntries(HashMap<Key, byte[]> entries) throws CosmianException;

}
