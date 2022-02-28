package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */
import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.internal.DrainingCloseable;
import lombok.Data;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Pattern;

// tag::javadoc[]

/**
 * Asynchronous / concurrent message consumer for Kafka.
 * <p>
 * Currently there is no direct implementation, only the {@link ParallelStreamProcessor} version (see {@link
 * AbstractParallelEoSStreamProcessor}), but there may be in the future.
 *
 * @param <K> key consume / produce key type
 * @param <V> value consume / produce value type
 * @see AbstractParallelEoSStreamProcessor
 * @see #poll(Consumer)
 */
// end::javadoc[]
public interface ParallelConsumer<K, V> extends DrainingCloseable {

    /**
     * @see org.apache.kafka.clients.consumer.KafkaConsumer#subscribe(Collection)
     */
    void subscribe(Collection<String> topics);

    /**
     * @see org.apache.kafka.clients.consumer.KafkaConsumer#subscribe(Pattern)
     */
    void subscribe(Pattern pattern);

    /**
     * @see org.apache.kafka.clients.consumer.KafkaConsumer#subscribe(Collection, ConsumerRebalanceListener)
     */
    void subscribe(Collection<String> topics, ConsumerRebalanceListener callback);

    /**
     * @see org.apache.kafka.clients.consumer.KafkaConsumer#subscribe(Pattern, ConsumerRebalanceListener)
     */
    void subscribe(Pattern pattern, ConsumerRebalanceListener callback);

    /**
     * Register a function to be applied to a batch of messages.
     * <p>
     * The system will treat the messages as a set, so if an error is thrown by the user code, then all messages will be
     * marked as failed and be retried (Note that when they are retried, there is no guarantee they will all be in the
     * same batch again). So if you're going to process messages individually, then don't use this function.
     * <p>
     * Otherwise, if you're going to process messages in sub sets from this batch, it's better to instead adjust the
     * {@link ParallelConsumerOptions#getBatchSize()} instead to the actual desired size, and process them as a whole.
     *
     * @see ParallelConsumerOptions#getBatchSize()
     */
    void pollBatch(Consumer<List<ConsumerRecord<K, V>>> usersVoidConsumptionFunction);

    /**
     * Pause this consumer (i.e. stop processing of messages).
     * <p>
     * This operation only has an effect if the consumer is currently running. In all other cases calling this method
     * will be silent a no-op.
     * </p>
     * <p>
     * Once the consumer is paused, the system will stop submitting work to the processing pool. Already submitted in
     * flight work however will be finished.
     * This includes work that is currently being processed inside a user function as well as work that has already
     * been submitted to the processing pool but has not been picked up by a free worker yet.
     * </p>
     * <p>
     * General remarks:
     * <ul>
     * <li>A paused consumer may still keep polling for new work until internal buffers are filled.</li>
     * <li>This operation does not actively pause the subscription on the underlying Kafka Broker (compared to
     * {@link org.apache.kafka.clients.consumer.KafkaConsumer#pause KafkaConsumer#pause}).</li>
     * <li>Pending offset commits will still be performed when the consumer is paused.</li>
     * </p>
     */
    void pauseIfRunning();

    /**
     * Resume this consumer (i.e. continue processing of messages).
     * <p>
     * This operation only has an effect if the consumer is currently paused. In all other cases calling this method
     * will be a silent no-op.
     * </p>
     */
    void resumeIfPaused();

    /**
     * A simple tuple structure.
     *
     * @param <L>
     * @param <R>
     */
    @Data
    class Tuple<L, R> {
        private final L left;
        private final R right;

        public static <LL, RR> Tuple<LL, RR> pairOf(LL l, RR r) {
            return new Tuple<>(l, r);
        }
    }

}
