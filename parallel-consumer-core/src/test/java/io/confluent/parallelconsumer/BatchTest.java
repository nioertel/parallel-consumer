package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.List;
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

    @ParameterizedTest
    @EnumSource
    @Test
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

    @Test
    public void test2() throws InterruptedException {
        int numRecs = 5;
        int batchSize = 2;
        super.setupParallelConsumerInstance(ParallelConsumerOptions.builder()
                .batchSize(batchSize)
                .ordering(ParallelConsumerOptions.ProcessingOrder.UNORDERED)
                .build());
        var recs = ktu.sendRecords(numRecs);
        recs.forEach(r -> log.info(r.toString()));
        List<List<ConsumerRecord<String, String>>> received = new ArrayList<>();
        List<ConsumerRecord<String, String>> records = new ArrayList<>();

        //
        parallelConsumer.pollBatch(x -> {
            log.info("Batch of messages: {}", toOffsets(x));
            received.add(x);
            records.addAll(x);
        });

        long start = System.currentTimeMillis();
        while (start + 1000 > System.currentTimeMillis()) {
            Thread.sleep(100);
            log.info(records.toString());
            log.info(String.valueOf(records.size()));
        }

        waitAtMost(ofSeconds(1)).alias("expected number of records")
                .untilAsserted(() -> {
                    assertThat(records).hasSize(5);
                });

        log.info(received.toString());
    }

    private List<Long> toOffsets(final List<ConsumerRecord<String, String>> x) {
        return x.stream().map(ConsumerRecord::offset).collect(Collectors.toList());
    }

}