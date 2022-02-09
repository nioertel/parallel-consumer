package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.internal.InternalRuntimeError;
import io.confluent.parallelconsumer.offsets.EncodingNotSupportedException;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import pl.tlinkowski.unij.api.UniMaps;
import pl.tlinkowski.unij.api.UniSets;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.confluent.csid.utils.KafkaUtils.toTopicPartition;
import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.offsets.OffsetMapCodecManager.DefaultMaxMetadataSize;

/**
 * In charge of managing {@link PartitionState}s.
 */
@Slf4j
@RequiredArgsConstructor
// todo rename to partition manager
public class PartitionMonitor<K, V> implements ConsumerRebalanceListener {

    /**
     * Best efforts attempt to prevent usage of offset payload beyond X% - as encoding size test is currently only done
     * per batch, we need to leave some buffer for the required space to overrun before hitting the hard limit where we
     * have to drop the offset payload entirely.
     */
    @Getter
    @Setter
    private double USED_PAYLOAD_THRESHOLD_MULTIPLIER = 0.75;

    private final Consumer<K, V> consumer;

    private final ShardManager<K, V> sm;

    /**
     * Hold the tracking state for each of our managed partitions.
     */
//    @Getter(PACKAGE)
    private final Map<TopicPartition, PartitionState<K, V>> partitionStates = new HashMap<>();

    /**
     * Record the generations of partition assignment, for fencing off invalid work.
     * <p>
     * This must live outside of {@link PartitionState}, as it must be tracked across partition lifecycles.
     * <p>
     * Starts at zero.
     */
    private final Map<TopicPartition, Integer> partitionsAssignmentEpochs = new HashMap<>();

    /**
     * Get's set to true whenever work is returned completed, so that we know when a commit needs to be made.
     * <p>
     * In normal operation, this probably makes very little difference, as typical commit frequency is 1 second, so low
     * chances no work has completed in the last second.
     */
    private final AtomicBoolean workStateIsDirtyNeedsCommitting = new AtomicBoolean(false);

    public Optional<PartitionState<K, V>> getPartitionState(TopicPartition tp) {
        // may cause the system to wait for a rebalance to finish
        PartitionState<K, V> kvPartitionState;
        synchronized (partitionStates) {
            kvPartitionState = partitionStates.get(tp);
        }
        return Optional.of(kvPartitionState);
    }

    /**
     * Load offset map for assigned partitions
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.debug("Partitions assigned: {}", partitions);
        synchronized (this.partitionStates) {

            for (final TopicPartition partition : partitions) {
                if (this.partitionStates.containsKey(partition))
                    log.warn("New assignment of partition {} which already exists in partition state. Could be a state bug.", partition);

            }

            incrementPartitionAssignmentEpoch(partitions);

            try {
                Set<TopicPartition> partitionsSet = UniSets.copyOf(partitions);
                OffsetMapCodecManager<K, V> om = new OffsetMapCodecManager<>(this.consumer); // todo remove throw away instance creation
                var partitionStates = om.loadOffsetMapForPartition(partitionsSet);
                this.partitionStates.putAll(partitionStates);
            } catch (Exception e) {
                log.error("Error in onPartitionsAssigned", e);
                throw e;
            }

        }
    }

    /**
     * Clear offset map for revoked partitions
     * <p>
     * {@link AbstractParallelEoSStreamProcessor#onPartitionsRevoked} handles committing off offsets upon revoke
     *
     * @see AbstractParallelEoSStreamProcessor#onPartitionsRevoked
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.info("Partitions revoked: {}", partitions);

        try {
            onPartitionsRemoved(partitions);
        } catch (Exception e) {
            log.error("Error in onPartitionsRevoked", e);
            throw e;
        }
    }

    void onPartitionsRemoved(final Collection<TopicPartition> partitions) {
        synchronized (this.partitionStates) {
            incrementPartitionAssignmentEpoch(partitions);
            resetOffsetMapAndRemoveWork(partitions);
        }
    }

    /**
     * Clear offset map for lost partitions
     */
    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        try {
            log.info("Lost partitions: {}", partitions);
            onPartitionsRemoved(partitions);
        } catch (Exception e) {
            log.error("Error in onPartitionsLost", e);
            throw e;
        }
    }

    /**
     * Truncate our tracked offsets as a commit was successful, so the low water mark rises, and we dont' need to track
     * as much anymore.
     * <p>
     * When commits are made to broker, we can throw away all the individually tracked offsets before the committed
     * offset.
     */
    public void onOffsetCommitSuccess(Map<TopicPartition, OffsetAndMetadata> offsetsToSend) {
        // partitionOffsetHighWaterMarks this will get overwritten in due course
        offsetsToSend.forEach((tp, meta) -> {
            var partition = getPartitionState(tp);
            partition.ifPresentOrElse(kvPartitionState -> kvPartitionState.onOffsetCommitSuccess(meta), () -> {
                log.error("onOffsetCommitSuccess event for revoked partition");
            });
        });
    }

    private void resetOffsetMapAndRemoveWork(Collection<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
            var partition = this.partitionStates.remove(tp);

            //
            NavigableMap<Long, WorkContainer<K, V>> oldWorkPartitionQueue = partition.getCommitQueues();
            if (oldWorkPartitionQueue != null) {
                sm.removeShardsFoundIn(oldWorkPartitionQueue);
            } else {
                log.trace("Removing empty commit queue");
            }
        }
    }

    public int getEpoch(final ConsumerRecord<K, V> rec) {
        var tp = toTopicPartition(rec);
        Integer epoch = partitionsAssignmentEpochs.get(tp);
        rec.topic();
        if (epoch == null) {
            throw new InternalRuntimeError(msg("Received message for a partition which is not assigned: {}", rec));
        }
        return epoch;
    }

    private void incrementPartitionAssignmentEpoch(final Collection<TopicPartition> partitions) {
        for (final TopicPartition partition : partitions) {
            int epoch = partitionsAssignmentEpochs.getOrDefault(partition, -1);
            epoch++;
            partitionsAssignmentEpochs.put(partition, epoch);
        }
    }

    /**
     * Have our partitions been revoked?
     *
     * @return true if epoch doesn't match, false if ok
     */
    boolean checkEpochIsStale(final WorkContainer<K, V> workContainer) {
        var topicPartitionKey = workContainer.getTopicPartition();

        Integer currentPartitionEpoch = partitionsAssignmentEpochs.get(topicPartitionKey);
        int workEpoch = workContainer.getEpoch();
        if (currentPartitionEpoch != workEpoch) {
            log.debug("Epoch mismatch {} vs {} for record {} - were partitions lost? Skipping message - it's already assigned to a different consumer (possibly me).",
                    workEpoch, currentPartitionEpoch, workContainer);
            return true;
        }
        return false;
    }


    private void maybeRaiseHighestSeenOffset(WorkContainer<K, V> wc) {
        maybeRaiseHighestSeenOffset(wc.getTopicPartition(), wc.offset());
    }

    public void maybeRaiseHighestSeenOffset(TopicPartition tp, long seenOffset) {
        Optional<PartitionState<K, V>> partitionState = getPartitionState(tp);
        partitionState.ifPresentOrElse(kvPartitionState -> kvPartitionState.maybeRaiseHighestSeenOffset(seenOffset), () -> {
            log.debug("Dropping offset raise {} for removed partition {}", seenOffset, tp);
        });
    }

    boolean isRecordPreviouslyCompleted(ConsumerRecord<K, V> rec) {
        var tp = toTopicPartition(rec);

        var partitionState = getPartitionState(tp);
        if (partitionState.isEmpty()) {
            // todo delete block - should never get here - should not receive records for which we have not registered partition state. Unless, we have received a message in the poll queue for a partition which we have since lost.
            int epoch = getEpoch(rec);
            log.error("No state found for partition {}, presuming message never before been processed. Partition epoch: {}", tp, epoch);
            return false;
        } else {
            boolean previouslyCompleted = partitionState.get().isRecordPreviouslyCompleted(rec);
            log.trace("Record {} previously completed? {}", rec.offset(), previouslyCompleted);
            return previouslyCompleted;
        }
    }

    public boolean isAllowedMoreRecords(TopicPartition tp) {
        Optional<PartitionState<K, V>> partitionState = getPartitionState(tp);
        return partitionState.map(PartitionState::isAllowedMoreRecords)
                // shouldn't reach this state, as doesn't make sesne, but block anyway
                .orElse(false);
    }

    public boolean hasWorkInCommitQueues() {
        for (var partition : this.partitionStates.values()) {
            if (partition.hasWorkInCommitQueue())
                return true;
        }
        return false;
    }

    public long getNumberOfEntriesInPartitionQueues() {
        return partitionStates.values().stream()
                .mapToLong(PartitionState::getCommitQueueSize)
                .reduce(Long::sum).orElse(0);
    }

    private void setPartitionMoreRecordsAllowedToProcess(TopicPartition topicPartitionKey, boolean moreMessagesAllowed) {
        var state = getPartitionState(topicPartitionKey);
        state.setAllowedMoreRecords(moreMessagesAllowed);
    }

    public Long getHighestSeenOffset(final TopicPartition tp) {
        return getPartitionState(tp).getOffsetHighestSeen();
    }

    public void addWorkContainer(final WorkContainer<K, V> wc) {
        maybeRaiseHighestSeenOffset(wc);
        var tp = wc.getTopicPartition();
        NavigableMap<Long, WorkContainer<K, V>> queue = getPartitionState(tp).getCommitQueues();
        queue.put(wc.offset(), wc);
    }

    /**
     * Checks if partition is blocked with back pressure.
     * <p>
     * If false, more messages are allowed to process for this partition.
     * <p>
     * If true, we have calculated that we can't record any more offsets for this partition, as our best performing
     * encoder requires nearly as much space is available for this partitions allocation of the maximum offset metadata
     * size.
     * <p>
     * Default (missing elements) is true - more messages can be processed.
     *
     * @see OffsetMapCodecManager#DefaultMaxMetadataSize
     */
    public boolean isBlocked(final TopicPartition topicPartition) {
        return !isAllowedMoreRecords(topicPartition);
    }

    /**
     * Get final offset data, build the offset map, and replace it in our map of offset data to send
     *
     * @param offsetsToSend
     * @param topicPartitionKey
     * @param incompleteOffsets
     */
    //todo refactor
    void addEncodedOffsets(Map<TopicPartition, OffsetAndMetadata> offsetsToSend,
                           TopicPartition topicPartitionKey,
                           LinkedHashSet<Long> incompleteOffsets) {
        // TODO potential optimisation: store/compare the current incomplete offsets to the last committed ones, to know if this step is needed or not (new progress has been made) - isdirty?
        boolean offsetEncodingNeeded = !incompleteOffsets.isEmpty();
        if (offsetEncodingNeeded) {
            // todo offsetOfNextExpectedMessage should be an attribute of State - consider deriving it from the state class
            long offsetOfNextExpectedMessage;
            OffsetAndMetadata finalOffsetOnly = offsetsToSend.get(topicPartitionKey);
            if (finalOffsetOnly == null) {
                // no new low watermark to commit, so use the last one again
                offsetOfNextExpectedMessage = incompleteOffsets.iterator().next(); // first element
            } else {
                offsetOfNextExpectedMessage = finalOffsetOnly.offset();
            }

            OffsetMapCodecManager<K, V> om = new OffsetMapCodecManager<>(this.consumer);
            try {
                PartitionState<K, V> state = getPartitionState(topicPartitionKey);
                // todo smelly - update the partition state with the new found incomplete offsets. This field is used by nested classes accessing the state
                state.setIncompleteOffsets(incompleteOffsets);
                String offsetMapPayload = om.makeOffsetMetadataPayload(offsetOfNextExpectedMessage, state);
                int metaPayloadLength = offsetMapPayload.length();
                boolean moreMessagesAllowed;
                OffsetAndMetadata offsetWithExtraMap;
                // todo move
                double pressureThresholdValue = DefaultMaxMetadataSize * USED_PAYLOAD_THRESHOLD_MULTIPLIER;

                if (metaPayloadLength > DefaultMaxMetadataSize) {
                    // exceeded maximum API allowed, strip the payload
                    moreMessagesAllowed = false;
                    offsetWithExtraMap = new OffsetAndMetadata(offsetOfNextExpectedMessage); // strip payload
                    log.warn("Offset map data too large (size: {}) to fit in metadata payload hard limit of {} - cannot include in commit. " +
                                    "Warning: messages might be replayed on rebalance. " +
                                    "See kafka.coordinator.group.OffsetConfig#DefaultMaxMetadataSize = {} and issue #47.",
                            metaPayloadLength, DefaultMaxMetadataSize, DefaultMaxMetadataSize);
                } else if (metaPayloadLength > pressureThresholdValue) { // and thus metaPayloadLength <= DefaultMaxMetadataSize
                    // try to turn on back pressure before max size is reached
                    moreMessagesAllowed = false;
                    offsetWithExtraMap = new OffsetAndMetadata(offsetOfNextExpectedMessage, offsetMapPayload);
                    log.warn("Payload size {} higher than threshold {}, but still lower than max {}. Will write payload, but will " +
                                    "not allow further messages, in order to allow the offset data to shrink (via succeeding messages).",
                            metaPayloadLength, pressureThresholdValue, DefaultMaxMetadataSize);
                } else { // and thus (metaPayloadLength <= pressureThresholdValue)
                    moreMessagesAllowed = true;
                    offsetWithExtraMap = new OffsetAndMetadata(offsetOfNextExpectedMessage, offsetMapPayload);
                    log.debug("Payload size {} within threshold {}", metaPayloadLength, pressureThresholdValue);
                }

                setPartitionMoreRecordsAllowedToProcess(topicPartitionKey, moreMessagesAllowed);
                offsetsToSend.put(topicPartitionKey, offsetWithExtraMap);
            } catch (EncodingNotSupportedException e) {
                setPartitionMoreRecordsAllowedToProcess(topicPartitionKey, false);
                log.warn("No encodings could be used to encode the offset map, skipping. Warning: messages might be replayed on rebalance.", e);
            }
        } else {
            setPartitionMoreRecordsAllowedToProcess(topicPartitionKey, true);
        }
    }

    public boolean isPartitionAssigned(ConsumerRecord<K, V> rec) {
        Optional<PartitionState<K, V>> partitionState = getPartitionState(toTopicPartition(rec));
        return partitionState.isPresent();
    }

    public void onSuccess(WorkContainer<K, V> wc) {
        Optional<PartitionState<K, V>> partitionState = getPartitionState(wc.getTopicPartition());
        if (partitionState.isPresent()) {
            partitionState.get().onSuccess(wc);
        } else {
            log.debug("Dropping completed work container for partition no longer assigned. WC: {}", wc);
        }
    }

    /**
     * Takes a record as work and puts it into internal queues, unless it's been previously recorded as completed as per
     * loaded records.
     *
     * @return true if the record was taken, false if it was skipped (previously successful)
     */
    boolean maybeRegisterNewRecordAsWork(final ConsumerRecord<K, V> rec) {
        if (rec == null) return false;

        if (!isPartitionAssigned(rec)) {
            log.debug("Record in buffer for a partition no longer assigned. Dropping. TP: {} rec: {}", toTopicPartition(rec), rec);
            return false;
        }

        if (isRecordPreviouslyCompleted(rec)) {
            log.trace("Record previously completed, skipping. offset: {}", rec.offset());
            return false;
        } else {
            int currentPartitionEpoch = getEpoch(rec);
            var wc = new WorkContainer<>(currentPartitionEpoch, rec);

            sm.addWorkContainer(wc);

            addWorkContainer(wc);

            return true;
        }
    }

    // todo rename
    public Map<TopicPartition, OffsetAndMetadata> findCompletedEligibleOffsetsAndRemove() {
        return findCompletedEligibleOffsetsAndRemove(true);
    }

    /**
     * TODO: This entire loop could be possibly redundant, if we instead track low water mark, and incomplete offsets as
     * work is submitted and returned.
     * <p>
     * todo: refactor into smaller methods?
     * <p>
     * todo docs
     * <p>
     * Finds eligible offset positions to commit in each assigned partition
     */
    // todo move to PartitionMonitor / PartitionState
    // todo rename
    // todo remove completely as state of offsets should be tracked live, no need to scan for them
    <R> Map<TopicPartition, OffsetAndMetadata> findCompletedEligibleOffsetsAndRemove(boolean remove) {

        //
        if (!isDirty()) {
            // nothing to commit
            return UniMaps.of();
        }

        //
        Map<TopicPartition, OffsetAndMetadata> offsetsToSend = new HashMap<>();
        int count = 0;
        int removed = 0;
        log.trace("Scanning for in order in-flight work that has completed...");

        //
        Set<Map.Entry<TopicPartition, PartitionState<K, V>>> set = this.partitionStates.entrySet();
        for (final Map.Entry<TopicPartition, PartitionState<K, V>> partitionStateEntry : set) {
            var partitionState = partitionStateEntry.getValue();
            Map<Long, WorkContainer<K, V>> partitionQueue = partitionState.getCommitQueues();
            TopicPartition topicPartitionKey = partitionStateEntry.getKey();
            log.trace("Starting scan of partition: {}", topicPartitionKey);

            count += partitionQueue.size();
            var workToRemove = new LinkedList<WorkContainer<K, V>>();
            var incompleteOffsets = new LinkedHashSet<Long>();
            long lowWaterMark = -1;
            var highestSucceeded = partitionState.getOffsetHighestSucceeded();
            // can't commit this offset or beyond, as this is the latest offset that is incomplete
            // i.e. only commit offsets that come before the current one, and stop looking for more
            boolean beyondSuccessiveSucceededOffsets = false;
            for (final var offsetAndItsWorkContainer : partitionQueue.entrySet()) {
                // ordered iteration via offset keys thanks to the tree-map
                WorkContainer<K, V> container = offsetAndItsWorkContainer.getValue();

                //
                long offset = container.getCr().offset();
                if (offset > highestSucceeded) {
                    break; // no more to encode
                }

                //
                boolean complete = container.isUserFunctionComplete();
                if (complete) {
                    if (container.getUserFunctionSucceeded().get() && !beyondSuccessiveSucceededOffsets) {
                        log.trace("Found offset candidate ({}) to add to offset commit map", container);
                        workToRemove.add(container);
                        // as in flights are processed in order, this will keep getting overwritten with the highest offset available
                        // current offset is the highest successful offset, so commit +1 - offset to be committed is defined as the offset of the next expected message to be read
                        long offsetOfNextExpectedMessageToBeCommitted = offset + 1;
                        OffsetAndMetadata offsetData = new OffsetAndMetadata(offsetOfNextExpectedMessageToBeCommitted);
                        offsetsToSend.put(topicPartitionKey, offsetData);
                    } else if (container.getUserFunctionSucceeded().get() && beyondSuccessiveSucceededOffsets) {
                        // todo lookup the low water mark and include here
                        log.trace("Offset {} is complete and succeeded, but we've iterated past the lowest committable offset ({}). Will mark as complete in the offset map.",
                                container.getCr().offset(), lowWaterMark);
                        // no-op - offset map is only for not succeeded or completed offsets
                    } else {
                        log.trace("Offset {} is complete, but failed processing. Will track in offset map as failed. Can't do normal offset commit past this point.", container.getCr().offset());
                        beyondSuccessiveSucceededOffsets = true;
                        incompleteOffsets.add(offset);
                    }
                } else {
                    lowWaterMark = container.offset();
                    beyondSuccessiveSucceededOffsets = true;
                    log.trace("Offset ({}) is incomplete, holding up the queue ({}) of size {}.",
                            container.getCr().offset(),
                            topicPartitionKey,
                            partitionQueue.size());
                    incompleteOffsets.add(offset);
                }
            }

            addEncodedOffsets(offsetsToSend, topicPartitionKey, incompleteOffsets);

            if (remove) {
                removed += workToRemove.size();
                for (var workContainer : workToRemove) {
                    var offset = workContainer.getCr().offset();
                    partitionQueue.remove(offset);
                }
            }
        }

        log.debug("Scan finished, {} were in flight, {} completed offsets removed, coalesced to {} offset(s) ({}) to be committed",
                count, removed, offsetsToSend.size(), offsetsToSend);
        return offsetsToSend;
    }

    public void setDirty() {
        workStateIsDirtyNeedsCommitting.set(true);
    }

    boolean isDirty() {
        return workStateIsDirtyNeedsCommitting.get();
    }

    public boolean isClean() {
        return !isDirty();
    }

}
