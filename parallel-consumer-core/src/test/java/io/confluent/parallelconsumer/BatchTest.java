package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.PARTITION;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.waitAtMost;

/**
 * Basic tests for batch processing functionality
 */
@Slf4j
public class BatchTest extends ParallelEoSStreamProcessorTestBase {

    @Test
    void averageBatchSizeTest() {
        int numRecs = 5000;
        int batchSize = 5;
        int maxConcurrency = 8;
        super.setupParallelConsumerInstance(ParallelConsumerOptions.builder()
                .batchSize(batchSize)
                .ordering(ParallelConsumerOptions.ProcessingOrder.UNORDERED)
                .maxConcurrency(maxConcurrency)
                .processorDelayMs(30)
                .initialDynamicLoadFactor(batchSize * 100)
                .build());
        ktu.sendRecords(numRecs);
        var numBatches = new AtomicInteger(0);
        var numRecords = new AtomicInteger(0);
        long start = System.currentTimeMillis();
        parallelConsumer.pollBatch(recordList -> {
            numBatches.getAndIncrement();
            numRecords.addAndGet(recordList.size());
            try {
                Thread.sleep(30);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (numRecords.get() % 100 == 0) {
                log.info(
                        "Processed {} records in {} batches with average size {}",
                        numRecords.get(),
                        numBatches.get(),
                        numRecords.get() / (0.0 + numBatches.get())
                );
            }
        });
        waitAtMost(ofSeconds(200)).alias("expected number of records")
                .untilAsserted(() -> {
                    assertThat(numRecords.get()).isEqualTo(numRecs);
                });
        var duration = System.currentTimeMillis() - start;
        log.info("Processed {} records in {} ms. This is {} records per second. " , numRecs, duration, numRecs / (duration / 1000.0));
        assertThat(numRecords.get() / (0.0 + numBatches.get())).isGreaterThan(batchSize * 0.9);
    }

    @ParameterizedTest
    @EnumSource
    void batch(ParallelConsumerOptions.ProcessingOrder order) {
        int numRecs = 5;
        int batchSize = 2;
        super.setupParallelConsumerInstance(ParallelConsumerOptions.builder()
                .batchSize(batchSize)
                .ordering(order)
                .build());
        var recs = ktu.sendRecords(numRecs);
        List<List<ConsumerRecord<String, String>>> received = new ArrayList<>();

        //
        parallelConsumer.pollBatch(x -> {
            log.info("Batch of messages: {}", toOffsets(x));
            received.add(x);
        });

        //
        int expectedNumOfBatches = (order == PARTITION) ?
                numRecs : // partition ordering restricts the batch sizes to a single element as all records are in a single partition
                (int) Math.ceil(numRecs / (double) batchSize);

        waitAtMost(ofSeconds(1)).alias("expected number of batches")
                .untilAsserted(() -> {
                    assertThat(received).hasSize(expectedNumOfBatches);
                });

        assertThat(received)
                .as("batch size")
                .allSatisfy(x -> assertThat(x).hasSizeLessThanOrEqualTo(batchSize))
                .as("all messages processed")
                .flatExtracting(x -> x).hasSameElementsAs(recs);
    }

    private List<Long> toOffsets(final List<ConsumerRecord<String, String>> x) {
        return x.stream().map(ConsumerRecord::offset).collect(Collectors.toList());
    }

}