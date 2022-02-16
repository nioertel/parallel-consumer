package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.KafkaUtils;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;

import java.util.NavigableMap;
import java.util.Set;

/**
 * No op version of {@link PartitionState} used for when partition assignments are removed
 *
 * @param <K>
 * @param <V>
 */
@Slf4j
public class RemovedPartitionState<K, V> extends PartitionState<K, V> {

    // todo warnings
//    private static final ConcurrentSkipListMap EMPTY_MAP = new ConcurrentSkipListMap<>();
//    private static final HashSet<Long> EMPTY_SET = new HashSet<>();

    // todo can set instance generics in a static context?
    private static final PartitionState singleton = new RemovedPartitionState();

//    public RemovedPartitionState(final TopicPartition tp, final OffsetMapCodecManager.HighestOffsetAndIncompletes incompletes) {
//        super(tp, incompletes);
//        throw new IllegalStateException();
//    }

    public RemovedPartitionState() {
        super(null, OffsetMapCodecManager.HighestOffsetAndIncompletes.of());
    }

    public static PartitionState getSingleton() {
        return RemovedPartitionState.singleton;
    }

    @Override
    boolean isAllowedMoreRecords() {
        log.debug("no-op");
        return false;
    }

    @Override
    public Set<Long> getIncompleteOffsets() {
        log.debug("no-op");
        // todo remove state access - invert
        return EMPTY_SET;
    }

    @Override
    public Long getOffsetHighestSeen() {
        log.debug("no-op");
        return 0L;
    }

    @Override
    public long getOffsetHighestSucceeded() {
        log.debug("no-op");
        return 0L;
    }

    @Override
    NavigableMap<Long, WorkContainer<K, V>> getCommitQueues() {
        log.debug("no-op");
        // todo remove state access - invert
        return EMPTY_MAP;
    }

    @Override
    public void setIncompleteOffsets(final Set<Long> incompleteOffsets) {
        log.debug("no-op");
    }

    @Override
    void setAllowedMoreRecords(final boolean allowedMoreRecords) {
        log.debug("no-op");
    }

    @Override
    public void maybeRaiseHighestSeenOffset(final long highestSeen) {
        log.debug("no-op");
    }

    @Override
    public void truncateOffsets(final long newLowWaterMark) {
        log.debug("no-op");
    }

    @Override
    public void onOffsetCommitSuccess(final OffsetAndMetadata meta) {
        log.debug("no-op");
    }

    @Override
    public boolean isRecordPreviouslyCompleted(final ConsumerRecord<K, V> rec) {
        log.debug("Ignoring previously completed request for partition no longer assigned. Partition: {}", KafkaUtils.toTopicPartition(rec));
        return false;
    }

    @Override
    public boolean hasWorkInCommitQueue() {
        return false;
    }

    @Override
    public int getCommitQueueSize() {
        return 0;
    }

    @Override
    public void onSuccess(final WorkContainer<K, V> work) {
        log.debug("Dropping completed work container for partition no longer assigned. WC: {}", work);
    }
}
