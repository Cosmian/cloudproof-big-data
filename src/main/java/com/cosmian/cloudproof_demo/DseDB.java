package com.cosmian.cloudproof_demo;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import com.cosmian.CosmianException;
import com.cosmian.cloudproof_demo.sse.DBInterface;
import com.cosmian.cloudproof_demo.sse.Sse.Key;
import com.cosmian.cloudproof_demo.sse.Sse.WordHash;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

/**
 * Implementation of the {@link DBInterface} for DSE 5.1.20 Documentation
 * https://docs.datastax.com/en/dse/5.1/cql/index.html and
 * https://docs.datastax.com/en/developer/java-driver/4.13/
 */
public class DseDB implements DBInterface, AutoCloseable {

    private static final Logger logger = Logger.getLogger(DseDB.class.getName());

    public static class Configuration {
        private final String ip;
        private final int port;
        private final String dataCenter;
        private final String username;
        private final String password;

        public Configuration(String ip, int port, String dataCenter, String username, String password) {
            this.ip = ip;
            this.port = port;
            this.dataCenter = dataCenter;
            this.username = username;
            this.password = password;
        }
    }

    final CqlSession session;

    /**
     * Connect to the local cassandra on 127.0.0.1:9042 at data center 'dc1'
     * 
     * @throws CosmianException if the local DSE cannot be contacted
     */
    public DseDB() throws CosmianException {
        this("127.0.0.1", 9042, "dc1", null, null);
    }

    /**
     * Instantiate a new DSE instance To know the data center: run 'select
     * data_center from system.local;'
     * 
     * @param config use the {@link Configuration} object to pass DSE config
     *               data
     * @throws CosmianException if the contact point canot be contacted
     */
    public DseDB(Configuration configuration) throws CosmianException {
        this(configuration.ip, configuration.port, configuration.dataCenter, configuration.username,
                configuration.password);
    }

    /**
     * Instantiate a new DSE instance To know the data center: run 'select
     * data_center from system.local;'
     * 
     * @param ipAddress  the address of the contact point
     * @param port       the port of the contact point
     * @param dataCenter the data center. Pass null if none
     * @param username   the username if any. Pass null if none
     * @param password   the password if any. Pass null is none
     * @throws CosmianException if the contact point canot be contacted
     */
    public DseDB(String ipAddress, int port, String dataCenter, String username, String password)
            throws CosmianException {
        try {
            CqlSessionBuilder sessionBuilder = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress(ipAddress, port));

            if (dataCenter != null) {
                sessionBuilder = sessionBuilder.withLocalDatacenter(dataCenter);
            }
            if (username != null) {
                sessionBuilder = sessionBuilder.withAuthCredentials(username, password);
            }
            CqlSession session = sessionBuilder.build();
            {
                session.execute("CREATE KEYSPACE IF NOT EXISTS cosmian_sse " + "WITH REPLICATION = { "
                        + "'class' : 'SimpleStrategy', " + "'replication_factor' : 1 " + "};");
                session.execute("CREATE TABLE IF NOT EXISTS cosmian_sse.entry_table "
                        + "(key text , ciphertext blob, PRIMARY KEY(key));");
                session.execute("CREATE TABLE IF NOT EXISTS cosmian_sse.chain_table "
                        + "(key text , ciphertext blob, PRIMARY KEY(key));");
            }
            this.session = session;
        } catch (Exception e) {
            throw new CosmianException("Failed initializing the DSE DB: " + e.getMessage(), e);
        }
    }

    @Override
    public HashMap<WordHash, byte[]> getEntryTableEntries(Set<WordHash> wordHashes) throws CosmianException {
        try {
            PreparedStatement prepared = session
                    .prepare("SELECT key, ciphertext from cosmian_sse.entry_table " + "WHERE key IN ?;");
            List<String> list = new ArrayList<String>();
            for (WordHash wh : wordHashes) {
                list.add(wh.toString());
            }
            BoundStatement bound = prepared.bind(list);
            ResultSet rs = this.session.execute(bound);
            HashMap<WordHash, byte[]> results = new HashMap<>();
            for (Row row : rs.all()) {
                results.put(WordHash.fromString(row.get(0, String.class)), row.get(1, ByteBuffer.class).array());
            }
            return results;
        } catch (Exception e) {
            throw new CosmianException("select in Entry Table failed: " + e.getMessage(), e);
        }
    }

    @Override
    public void upsertEntryTableEntries(HashMap<WordHash, byte[]> entries) throws CosmianException {
        try {
            PreparedStatement upsert = session
                    .prepare("INSERT INTO cosmian_sse.entry_table (key, ciphertext) " + "VALUES (:key, :ciphertext)");
            BatchStatementBuilder batchBuilder = BatchStatement.builder(DefaultBatchType.LOGGED);
            for (Map.Entry<WordHash, byte[]> entry : entries.entrySet()) {
                batchBuilder.addStatement(upsert.bind(entry.getKey().toString(), ByteBuffer.wrap(entry.getValue())));
            }
            BatchStatement batch = batchBuilder.build();
            this.session.execute(batch);
        } catch (Exception e) {
            throw new CosmianException("upsert in Entry Table failed: " + e.getMessage(), e);
        }
    }

    public void truncateEntryTable() throws CosmianException {
        try {
            this.session.execute("TRUNCATE cosmian_sse.entry_table;");
        } catch (Exception e) {
            throw new CosmianException("truncate of Entry Table failed: " + e.getMessage(), e);
        }
    }

    public long entryTableSize() throws CosmianException {
        try {
            ResultSet rs = this.session.execute("SELECT COUNT(*) FROM cosmian_sse.entry_table;");
            Row row = rs.one();
            return row.getLong(0);
        } catch (Exception e) {
            throw new CosmianException("Count of Entry Table failed: " + e.getMessage(), e);
        }
    }

    @Override
    public Set<byte[]> getChainTableEntries(Set<Key> chainTableKeys) throws CosmianException {
        try {
            PreparedStatement prepared = session
                    .prepare("SELECT key, ciphertext from cosmian_sse.chain_table " + "WHERE key IN ?;");
            List<String> list = new ArrayList<String>();
            for (Key key : chainTableKeys) {
                list.add(key.toString());
            }
            BoundStatement bound = prepared.bind(list);
            ResultSet rs = this.session.execute(bound);
            Set<byte[]> results = new HashSet<>();
            for (Row row : rs.all()) {
                results.add(row.get(1, ByteBuffer.class).array());
            }
            return results;
        } catch (Exception e) {
            throw new CosmianException("select in Chain Table failed: " + e.getMessage(), e);
        }
    }

    @Override
    public void upsertChainTableEntries(HashMap<Key, byte[]> entries) throws CosmianException {
        try {
            PreparedStatement upsert = session
                    .prepare("INSERT INTO cosmian_sse.chain_table (key, ciphertext) " + "VALUES (:key, :ciphertext)");
            BatchStatementBuilder batchBuilder = BatchStatement.builder(DefaultBatchType.LOGGED);
            for (Map.Entry<Key, byte[]> entry : entries.entrySet()) {
                batchBuilder.addStatement(upsert.bind(entry.getKey().toString(), ByteBuffer.wrap(entry.getValue())));
            }
            BatchStatement batch = batchBuilder.build();
            this.session.execute(batch);
        } catch (Exception e) {
            throw new CosmianException("upsert in Chain Table failed: " + e.getMessage(), e);
        }

    }

    public void truncateChainTable() throws CosmianException {
        try {
            this.session.execute("TRUNCATE cosmian_sse.chain_table;");
        } catch (Exception e) {
            throw new CosmianException("truncate of Entry Table failed: " + e.getMessage(), e);
        }
    }

    public long chainTableSize() throws CosmianException {
        try {
            ResultSet rs = this.session.execute("SELECT COUNT(*) FROM cosmian_sse.chain_table;");
            Row row = rs.one();
            return row.getLong(0);
        } catch (Exception e) {
            throw new CosmianException("Count of Entry Table failed: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        if (this.session != null) {
            try {
                this.session.close();
            } catch (Exception e) {
                logger.warning(() -> "Failed closing the DSE session: " + e.getMessage());
            }
        }
    }

}
