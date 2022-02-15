package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.ProgressBarUtils;
import io.confluent.csid.utils.ProgressTracker;
import io.confluent.csid.utils.TrimListRepresentation;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.internal.StandardComparisonStrategy;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.MDC;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor.MDC_INSTANCE_ID;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.util.IterableUtil.toCollection;
import static org.awaitility.Awaitility.waitAtMost;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Test running with multiple instances of parallel-consumer consuming from topic with two partitions.
 */
//@Isolated
//@Execution(ExecutionMode.SAME_THREAD)
@Slf4j
class MultiInstanceRebalanceTest extends BrokerIntegrationTest<String, String> {

    static final int DEFAULT_MAX_POLL = 1;
    AtomicInteger count = new AtomicInteger();

    static {
        MDC.put(MDC_INSTANCE_ID, "Test-Thread");
    }

    @ParameterizedTest
    @EnumSource(ProcessingOrder.class)
    void consumeWithMultipleInstancesPeriodicConsumerSync(ProcessingOrder order) {
        numPartitions = 2;
        int expectedMessageCount = 100;
        int numberOfPcsToRun = 2;
        runTest(DEFAULT_MAX_POLL, CommitMode.PERIODIC_CONSUMER_SYNC, order, expectedMessageCount, numberOfPcsToRun);
    }

    @ParameterizedTest
    @EnumSource(ProcessingOrder.class)
    void consumeWithMultipleInstancesPeriodicConsumerAsync(ProcessingOrder order) {
        numPartitions = 2;
        int expectedMessageCount = 100;
        runTest(DEFAULT_MAX_POLL, CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS, order, expectedMessageCount, 2);
    }

    /**
     * Tests with very large numbers of parallel consumer instances to try to reproduce state and concurrency issues
     * (#188, #189)
     */
    @Test
    void largeNumberOfInstances() {
        numPartitions = 80;
        int numberOfPcsToRun = 9;
        int expectedMessageCount = 100;
        runTest(DEFAULT_MAX_POLL, CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS, ProcessingOrder.UNORDERED, expectedMessageCount, numberOfPcsToRun);
    }

    @SneakyThrows
    private void runTest(int maxPoll, CommitMode commitMode, ProcessingOrder order, int expectedMessageCount, int numberOfPcsToRun) {
        String inputName = setupTopic(this.getClass().getSimpleName() + "-input-" + RandomUtils.nextInt());

        ExecutorService pcExecutor = Executors.newFixedThreadPool(2);

        // Submit first parallel-consumer
        log.info("Running first instance of pc");
        ProgressBar bar1 = ProgressBarUtils.getNewMessagesBar("PC1", log, expectedMessageCount);
        ParallelConsumerRunnable pc1 = new ParallelConsumerRunnable(maxPoll, commitMode, order, inputName, bar1);
        pcExecutor.submit(pc1);

        // pre-produce messages to input-topic
        Queue<String> expectedKeys = new ConcurrentLinkedQueue<>();
        log.info("Producing {} messages before starting test", expectedMessageCount);
        List<Future<RecordMetadata>> sends = new ArrayList<>();
        try (Producer<String, String> kafkaProducer = kcu.createNewProducer(false)) {
            for (int i = 0; i < expectedMessageCount; i++) {
                String key = "key-" + i;
                Future<RecordMetadata> send = kafkaProducer.send(new ProducerRecord<>(inputName, key, "value-" + i), (meta, exception) -> {
                    if (exception != null) {
                        log.error("Error sending, ", exception);
                    }
                });
                sends.add(send);
                expectedKeys.add(key);
            }
            log.debug("Finished sending test data");
        }

        // make sure we finish sending before next stage
        log.debug("Waiting for broker acks");
        for (Future<RecordMetadata> send : sends) {
            send.get();
        }
        assertThat(sends).hasSize(expectedMessageCount);

        // run parallel-consumer
        log.debug("Starting test");

        ProgressBar bar2 = ProgressBarUtils.getNewMessagesBar("PC2", log, expectedMessageCount);

        // Wait for first consumer to consume messages, also effectively waits for the group.initial.rebalance.delay.ms (3s by default)
        Awaitility.waitAtMost(ofSeconds(10))
                .until(() -> pc1.getConsumedKeys().size() > 10);

//        var sender = new Runnable(){
//
//            @SneakyThrows
//            @Override
//            public void run() {
//                // pre-produce messages to input-topic
//                log.info("Producing {} messages before starting test", expectedMessageCount);
//                try (Producer<String, String> kafkaProducer = kcu.createNewProducer(false)) {
//                    for (int i = 0; i < expectedMessageCount*1000; i++) {
//                        Thread.sleep(1);
//                        String key = "key-" + i;
//                        log.debug("sending {}", key);
//                        Future<RecordMetadata> send = kafkaProducer.send(new ProducerRecord<>(inputName, key, "value-" + i), (meta, exception) -> {
//                            if (exception != null) {
//                                log.error("Error sending, ", exception);
//                            }
//                        });
//                        sends.add(send);
//                        expectedKeys.add(key);
//                    }
//                    log.debug("Finished sending test data");
//                }
//            }
//        };
//        pcExecutor.submit(sender);

        var secondaryPcs = IntStream.range(1, numberOfPcsToRun)
                .mapToObj(value -> {
                            try {
                                int jitterRangeMs = 2000;
                                Thread.sleep((int) (Math.random() * jitterRangeMs)); // jitter pc start
                            } catch (InterruptedException e) {
                                log.error(e.getMessage(), e);
                            }
                            log.info("Running pc instance {}", value);
                            ParallelConsumerRunnable instance = new ParallelConsumerRunnable(maxPoll, commitMode, order, inputName, bar2);
                            pcExecutor.submit(instance);
                            return instance;
                        }
                ).collect(Collectors.toList());
        var runnables = new ArrayList<ParallelConsumerRunnable>();
        runnables.add(pc1);
        runnables.addAll(secondaryPcs);
        final ParallelConsumerRunnable[] parallelConsumerRunnablesArray = runnables.toArray(new ParallelConsumerRunnable[0]);


        // wait for all pre-produced messages to be processed and produced
        Assertions.useRepresentation(new TrimListRepresentation());
        var failureMessage = msg("All keys sent to input-topic should be processed, within time (expected: {} commit: {} order: {} max poll: {})",
                expectedMessageCount, commitMode, order, maxPoll);
        ProgressTracker progressTracker = new ProgressTracker(count);
        try {
            waitAtMost(ofSeconds(30))
                    // dynamic reason support still waiting https://github.com/awaitility/awaitility/pull/193#issuecomment-873116199
                    // .failFast( () -> pc1.getFailureCause(), () -> pc1.isClosedOrFailed()) // requires https://github.com/awaitility/awaitility/issues/178#issuecomment-734769761
                    .failFast("PC died - check logs", () -> pc1.getParallelConsumer().isClosedOrFailed()) // requires https://github.com/awaitility/awaitility/issues/178#issuecomment-734769761
                    .alias(failureMessage)
                    .pollInterval(1, SECONDS)
                    .untilAsserted(() -> {
                        log.trace("Processed-count: {}", getAllConsumedKeys(parallelConsumerRunnablesArray).size());
                        if (progressTracker.hasProgressNotBeenMade()) {
                            expectedKeys.removeAll(getAllConsumedKeys(parallelConsumerRunnablesArray));
                            throw progressTracker.constructError(msg("No progress, missing keys: {}.", expectedKeys));
                        }
                        SoftAssertions all = new SoftAssertions();
                        all.assertThat(getAllConsumedKeys(parallelConsumerRunnablesArray)).as("all expected are consumed").containsAll(expectedKeys);
                        // NB: Re-balance causes re-processing, and this is probably expected. Leaving test like this anyway
                        all.assertThat(getAllConsumedKeys(parallelConsumerRunnablesArray)).as("all expected are consumed only once").hasSizeGreaterThanOrEqualTo(expectedKeys.size());

                        all.assertAll();
                    });
        } catch (ConditionTimeoutException e) {
            fail(failureMessage + "\n" + e.getMessage());
        }

        bar1.close();
        bar2.close();

        runnables.forEach(pc -> pc.getParallelConsumer().closeDrainFirst());

        assertThat(pc1.consumedKeys).hasSizeGreaterThan(0);
        assertThat(getAllConsumedKeys(secondaryPcs.toArray(new ParallelConsumerRunnable[0])))
                .as("Second PC should have taken over some of the work and consumed some records")
                .hasSizeGreaterThan(0);

        pcExecutor.shutdown();

        Collection<?> duplicates = toCollection(StandardComparisonStrategy.instance()
                .duplicatesFrom(getAllConsumedKeys(parallelConsumerRunnablesArray)));
        log.info("Duplicate consumed keys (at least one is expected due to the rebalance): {}", duplicates);
        assertThat(duplicates)
                .as("There should be few duplicate keys")
                .hasSizeLessThan(10); // in some env, there are a lot more. i.e. Jenkins running parallel suits
    }

    List<String> getAllConsumedKeys(ParallelConsumerRunnable... instances) {
        return Arrays.stream(instances)
                .flatMap(parallelConsumerRunnable -> parallelConsumerRunnable.consumedKeys.stream())
                .collect(Collectors.toList());
    }

    int instanceId = 0;

    @Getter
    @RequiredArgsConstructor
    class ParallelConsumerRunnable implements Runnable {

        private final int maxPoll;
        private final CommitMode commitMode;
        private final ProcessingOrder order;
        private final String inputTopic;
        private final ProgressBar bar;
        private ParallelEoSStreamProcessor<String, String> parallelConsumer;
        private final Queue<String> consumedKeys = new ConcurrentLinkedQueue<>();

        @Override
        public void run() {
            MDC.put(MDC_INSTANCE_ID, "Runner-" + instanceId);
            log.info("Running consumer!");

            Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPoll);
//            consumerProps.put(ConsumerConfig.DE, OffsetResetStrategy.EARLIEST.name().toLowerCase());
            KafkaConsumer<String, String> newConsumer = kcu.createNewConsumer(false, consumerProps);

            this.parallelConsumer = new ParallelEoSStreamProcessor<>(ParallelConsumerOptions.<String, String>builder()
                    .ordering(order)
//                    .logId(Integer.toString(instanceId))
                    .consumer(newConsumer)
                    .commitMode(commitMode)
                    .maxConcurrency(1)
                    .build());

            parallelConsumer.setMyId(Optional.of("PC-" + instanceId));

            instanceId++;

            parallelConsumer.subscribe(of(inputTopic));

            parallelConsumer.poll(record -> {
                        // simulate work
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                            // ignore
                        }
                        count.incrementAndGet();
                        bar.stepBy(1);
                        consumedKeys.add(record.key());
                    }
            );
        }
    }


}
