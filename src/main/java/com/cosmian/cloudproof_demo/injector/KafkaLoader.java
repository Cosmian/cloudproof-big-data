package com.cosmian.cloudproof_demo.injector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.cosmian.cloudproof_demo.AppException;
import com.cosmian.cloudproof_demo.fs.InputPath;

public class KafkaLoader {

    private static final Logger logger = Logger.getLogger(KafkaLoader.class.getName());

    final Properties props;

    final List<String> inputs;

    public KafkaLoader(Properties props, List<String> inputs) {
        this.props = props;
        this.inputs = inputs;
    }

    public void run() throws AppException {
        int numLines = 0;
        String topic = props.getProperty("topic");
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            for (String inputPathString : inputs) {
                InputPath inputPath = InputPath.parse(inputPathString);
                Iterator<String> it = inputPath.listFiles();
                while (it.hasNext()) {
                    String inputFile = it.next();
                    try {

                        InputStream is = inputPath.getFs().getInputStream(inputFile);

                        try (BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
                            String line;
                            while ((line = br.readLine()) != null) {
                                if (line.trim().length() == 0) {
                                    continue;
                                }
                                producer.send(new ProducerRecord<String, String>(topic, line));
                                numLines += 1;
                            }
                        } catch (IOException e) {
                            throw new AppException("an /IO Error occurred:" + e.getMessage(), e);
                        }

                    } catch (AppException e) {
                        logger.severe("Aborting processing of the file: " + inputFile + ": " + e.getMessage());
                    }
                }
            }
        }
        final int numLinesFinal = numLines;
        logger.info(() -> "Loaded " + numLinesFinal + " lines in topic " + topic);
    }

}
