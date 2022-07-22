package com.cosmian.cloudproof_demo.injector;

import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.cosmian.cloudproof_demo.AppException;

public class KafkaLineReader implements LineReader {

    private static final Logger logger = Logger.getLogger(KafkaLineReader.class.getName());

    final List<String> topics;

    Consumer<String, String> consumer;

    Iterator<ConsumerRecord<String, String>> records;

    long totalRecordsProcessed;

    public KafkaLineReader(List<String> topics) {
        this.topics = topics;
        this.consumer = null;
        this.records = null;
        this.totalRecordsProcessed = 0;
    }

    @Override
    public void close() throws IOException {
        if (this.consumer != null) {
            this.consumer.close();
        }
    }

    @Override
    public String readNext() throws AppException {
        if (consumer == null) {
            getConsumer();
        }
        if (records == null || !records.hasNext()) {
            poll();
        }
        String line = records.next().value();
        if (line.equals("%END%")) {
            return null;
        }
        this.totalRecordsProcessed += 1;
        return line;
    }

    private void getConsumer() throws AppException {
        // try reading kafka.properties file
        Properties props = new Properties();
        try {
            props.load(new FileReader("./kafka.properties"));
        } catch (IOException e) {
            throw new AppException("Unable to load the kafka.properties files: " + e.getMessage(), e);
        }

        // Build the list of topics to subscribe from
        HashSet<String> topicsSet = new HashSet<>();
        if (this.topics != null) {
            topicsSet.addAll(this.topics);
        }
        String confTopic = props.getProperty("topic");
        if (confTopic != null) {
            topicsSet.add(confTopic);
        }
        if (topicsSet.isEmpty()) {
            throw new AppException(
                "At least one topic must be supplied via the command line arguments or the kafka.properties files ");
        }

        // instantiate the Kafka consumer
        this.consumer = new KafkaConsumer<>(props);
        // and subscribe to the topic(s)
        consumer.subscribe(topicsSet);
        this.records = null;
    }

    private void poll() {
        while (true) {
            final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            if (!records.isEmpty()) {
                logger.info(() -> "Loaded " + records.count() + " records from Kafka topics ("
                    + this.totalRecordsProcessed + " already processed)");
                this.records = records.iterator();
                return;
            }
            consumer.commitAsync();
        }
    }

}
