package com.cosmian.cloudproof_demo.injector;

import java.util.List;
import java.util.logging.Logger;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import com.cosmian.cloudproof_demo.AppException;
import com.cosmian.cloudproof_demo.Benchmarks;
import com.cosmian.cloudproof_demo.DseDB.Configuration;
import com.cosmian.cloudproof_demo.sse.Sse.Key;
import com.cosmian.cloudproof_demo.sse.SseUpserter;

public class SparkInjector implements Injector {

    private static final Logger logger = Logger.getLogger(SparkInjector.class.getName());

    private final JavaSparkContext spark;

    private final Benchmarks benchmarks = new Benchmarks();

    public SparkInjector(JavaSparkContext spark) {
        this.spark = spark;
    }

    @Override
    public void run(Key k, Key kStar, String publicKeyJson, String outputDirectory, Configuration dseConf,
        List<String> inputs, boolean kafka, int maxSizeInMB, int maxAgeInSeconds, boolean dropIndexes)
        throws AppException {

        benchmarks.startRecording("total_time");

        // Create a cache of the Public Key and Policy
        benchmarks.startRecording("init");

        // truncate the DSE Index Tables if needed
        if (dropIndexes) {
            SseUpserter.truncate(dseConf);
        }

        benchmarks.stopRecording("init", 1);

        benchmarks.startRecording("record_process_time");
        long numRecords = 0;
        try {
            for (String inputPathString : inputs) {
                LongAccumulator counter = spark.sc().longAccumulator();
                JavaRDD<String> inputRdd = spark.textFile(inputPathString).cache();
                inputRdd.foreachPartition(new SparkInjectionProcess(k, kStar, publicKeyJson, dseConf, outputDirectory,
                    counter, maxSizeInMB, maxAgeInSeconds));
                numRecords += counter.value();
            }
        } finally {
            benchmarks.stopRecording("record_process_time", numRecords);
            benchmarks.stopRecording("total_time", 1);
            logBenchmarks(benchmarks);
        }
    }

    static void logBenchmarks(Benchmarks benchmarks) {
        logger.info(() -> {
            StringBuilder builder = new StringBuilder();
            long total_records = (long) benchmarks.getCount("record_process_time");
            long total_time = (long) benchmarks.getSum("total_time");

            builder.append("Processed ").append(String.format("%,d", total_records)).append(" records in ")
                .append(String.format("%,d", total_time / 1000)).append("ms (init: ")
                .append(String.format("%,.1f", benchmarks.getSum("init") / total_time * 100.0)).append("%, records: ")
                .append(String.format("%,.1f", benchmarks.getSum("record_process_time") / total_time * 100.0))
                .append("%)\n");

            double record_total = benchmarks.getAverage("record_process_time") / 1000.0;
            builder.append("Average record total: ").append(String.format("%,.3f", record_total)).append("ms\n");
            return builder.toString();
        });
    }

}
