package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import io.confluent.parallelconsumer.state.WorkManager;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.MDC;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.*;

import static io.confluent.csid.utils.BackportUtils.toSeconds;
import static io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor.MDC_INSTANCE_ID;
import static io.confluent.parallelconsumer.internal.State.closed;
import static io.confluent.parallelconsumer.internal.State.running;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Subsystem for polling the broker for messages.
 *
 * @param <K>
 * @param <V>
 */
@Slf4j
public class BrokerPollSystem<K, V> implements OffsetCommitter {

    private final ConsumerManager<K, V> consumerManager;

    private State state = running;

    private Optional<Future<Boolean>> pollControlThreadFuture;

    /**
     * While {@link io.confluent.parallelconsumer.internal.State#paused paused} is an externally controlled state that
     * temporarily stops polling and work registration, the {@code paused} flag is used internally to pause subscriptions
     * if polling needs to be throttled.
     */
    @Getter
    private volatile boolean paused = false;

    private final AbstractParallelEoSStreamProcessor<K, V> pc;

    private Optional<ConsumerOffsetCommitter<K, V>> committer = Optional.empty();

    /**
     * Note how this relates to {@link BrokerPollSystem#getLongPollTimeout()} - if longPollTimeout is high and loading
     * factor is low, there may not be enough messages queued up to satisfy demand.
     */
    @Setter
    @Getter
    private static Duration longPollTimeout = Duration.ofMillis(2000);

    private final WorkManager<K, V> wm;

    public BrokerPollSystem(ConsumerManager<K, V> consumerMgr, WorkManager<K, V> wm, AbstractParallelEoSStreamProcessor<K, V> pc, final ParallelConsumerOptions options) {
        this.wm = wm;
        this.pc = pc;

        this.consumerManager = consumerMgr;
        switch (options.getCommitMode()) {
            case PERIODIC_CONSUMER_SYNC, PERIODIC_CONSUMER_ASYNCHRONOUS -> {
                ConsumerOffsetCommitter<K, V> consumerCommitter = new ConsumerOffsetCommitter<>(consumerMgr, wm, options);
                committer = Optional.of(consumerCommitter);
            }
        }
    }

    public void start(String managedExecutorService) {
        ExecutorService executorService;
        try {
            executorService = InitialContext.doLookup(managedExecutorService);
        } catch (NamingException e) {
            log.debug("Using Java SE Thread",e);
            executorService = Executors.newSingleThreadExecutor();
        }
        Future<Boolean> submit = executorService.submit(this::controlLoop);
        this.pollControlThreadFuture = Optional.of(submit);
    }

    public void supervise() {
        if (pollControlThreadFuture.isPresent()) {
            Future<Boolean> booleanFuture = pollControlThreadFuture.get();
            if (booleanFuture.isCancelled() || booleanFuture.isDone()) {
                try {
                    booleanFuture.get();
                } catch (Exception e) {
                    throw new InternalRuntimeError("Error in " + BrokerPollSystem.class.getSimpleName() + " system.", e);
                }
            }
        }
    }

    /**
     * @return true if closed cleanly
     */
    private boolean controlLoop() {
        Thread.currentThread().setName("pc-broker-poll");
        pc.getMyId().ifPresent(id -> MDC.put(MDC_INSTANCE_ID, id));
        log.trace("Broker poll control loop start");
        committer.ifPresent(x -> x.claim());
        try {
            while (state != closed) {
                log.trace("Loop: Broker poller: ({})", state);
                if (state == running) {
                    ConsumerRecords<K, V> polledRecords = pollBrokerForRecords();
                    log.debug("Got {} records in poll result", polledRecords.count());

                    if (!polledRecords.isEmpty()) {
                        log.trace("Loop: Register work");
                        wm.registerWork(polledRecords);

                        // notify control work has been registered, in case it's sleeping waiting for work that will never come
                        if (!wm.hasWorkInFlight()) {
                            log.trace("Apparently no work is being done, make sure Control is awake to receive messages");
                            pc.notifySomethingToDo();
                        }
                    }
                }

                maybeDoCommit();

                switch (state) {
                    case draining -> {
                        doPause();
                        transitionToCloseMaybe();
                    }
                    case closing -> {
                        doClose();
                    }
                }

                if (state == State.paused) {
                    // as long as the state is paused, this loop doesn't do any I/O work
                    // therefor we sleep for 100 ms to reduce CPU load
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        log.debug("Loop: Sleep in state paused was interrupted.");
                        // swallow the exception and just go back to business, keeping the interrupt state set
                        Thread.currentThread().interrupt();
                    }
                }
            }
            log.debug("Broker poll thread finished, returning true to future");
            return true;
        } catch (Exception e) {
            log.error("Unknown error", e);
            throw e;
        }
    }

    private void transitionToCloseMaybe() {
        // make sure everything is committed
        if (isResponsibleForCommits() && !wm.isRecordsAwaitingToBeCommitted()) {
            // transition to closing
            state = State.closing;
        } else {
            log.trace("Draining, but work still needs to be committed. Yielding thread to avoid busy wait.");
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void doClose() {
        doPause();
        maybeCloseConsumer();
        state = closed;
    }

    /**
     * To keep things simple, make sure the correct thread which can make a commit, is the one to close the consumer.
     * This way, if partitions are revoked, the commit can be made inline.
     */
    private void maybeCloseConsumer() {
        if (isResponsibleForCommits()) {
            log.debug("Closing {}, first closing consumer...", this.getClass().getSimpleName());
            this.consumerManager.close(DrainingCloseable.DEFAULT_TIMEOUT);
            log.debug("Consumer closed.");
        }
    }

    private boolean isResponsibleForCommits() {
        return committer.isPresent();
    }

    private ConsumerRecords<K, V> pollBrokerForRecords() {
        managePauseOfSubscription();
        log.debug("Subscriptions are paused: {}", paused);

        Duration thisLongPollTimeout = state == running ? BrokerPollSystem.longPollTimeout
                : Duration.ofMillis(1); // Can't use Duration.ZERO - this causes Object#wait to wait forever

        log.debug("Long polling broker with timeout {} seconds, might appear to sleep here if subs are paused, or no data available on broker.", toSeconds(thisLongPollTimeout));
        return consumerManager.poll(thisLongPollTimeout);
    }

    /**
     * Will begin the shutdown process, eventually closing itself once drained
     */
    public void drain() {
        // idempotent
        if (state != State.draining) {
            log.debug("Signaling poll system to drain, waking up consumer...");
            state = State.draining;
            consumerManager.wakeup();
        }
    }

    private final RateLimiter pauseLimiter = new RateLimiter(1);

    private void doPause() {
        // idempotent
        if (paused) {
            log.trace("Already paused");
        } else {
            if (pauseLimiter.couldPerform()) {
                pauseLimiter.performIfNotLimited(() -> {
                    paused = true;
                    log.debug("Pausing subs");
                    Set<TopicPartition> assignment = consumerManager.assignment();
                    consumerManager.pause(assignment);
                });
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Should pause but pause rate limit exceeded {} vs {}. Queued: {}",
                            pauseLimiter.getElapsedDuration(),
                            pauseLimiter.getRate(),
                            wm.getWorkQueuedInMailboxCount());
                }
            }
        }
    }

    public void closeAndWait() throws TimeoutException, ExecutionException {
        log.debug("Requesting broker polling system to close...");
        transitionToClosing();
        if (pollControlThreadFuture.isPresent()) {
            log.debug("Wait for loop to finish ending...");
            Future<Boolean> pollControlResult = pollControlThreadFuture.get();
            boolean interrupted = true;
            while (interrupted) {
                try {
                    Boolean pollShutdownSuccess = pollControlResult.get(DrainingCloseable.DEFAULT_TIMEOUT.toMillis(), MILLISECONDS);
                    interrupted = false;
                    if (!pollShutdownSuccess) {
                        log.warn("Broker poll control thread not closed cleanly.");
                    }
                } catch (InterruptedException e) {
                    log.debug("Interrupted waiting for broker poller thread to finish", e);
                } catch (ExecutionException | TimeoutException e) {
                    log.error("Execution or timeout exception waiting for broker poller thread to finish", e);
                    throw e;
                }
            }
        }
        log.debug("Broker poll system finished closing");
    }

    private void transitionToClosing() {
        log.debug("Poller transitioning to closing, waking up consumer");
        state = State.closing;
        consumerManager.wakeup();
    }

    /**
     * If we are currently processing too many records, we must stop polling for more from the broker. But we must also
     * make sure we maintain the keep alive with the broker so as not to cause a rebalance.
     */
    private void managePauseOfSubscription() {
        boolean throttle = shouldThrottle();
        log.trace("Need to throttle: {}", throttle);
        if (throttle) {
            doPause();
        } else {
            resumeIfPaused();
        }
    }

    /**
     * Has no flap limit, always resume if we need to
     */
    private void resumeIfPaused() {
        // idempotent
        if (paused) {
            log.debug("Resuming consumer, waking up");
            Set<TopicPartition> pausedTopics = consumerManager.paused();
            consumerManager.resume(pausedTopics);
            // trigger consumer to perform a new poll without the assignments paused, otherwise it will continue to long poll on nothing
            consumerManager.wakeup();
            paused = false;
        }
    }

    private boolean shouldThrottle() {
        return wm.shouldThrottle();
    }

    /**
     * Optionally blocks. Threadsafe
     *
     * @see CommitMode
     */
    @SneakyThrows
    @Override
    public void retrieveOffsetsAndCommit() {
        // {@link Optional#ifPresentOrElse} only @since 9
        ConsumerOffsetCommitter<K, V> committer = this.committer.orElseThrow(() -> {
            // shouldn't be here
            throw new IllegalStateException("No committer configured");
        });
        committer.commit();
    }

    /**
     * Will silently skip if not configured with a committer
     */
    private void maybeDoCommit() {
        committer.ifPresent(ConsumerOffsetCommitter::maybeDoCommit);
    }

    /**
     * Wakeup if colling the broker
     */
    public void wakeupIfPaused() {
        if (paused)
            consumerManager.wakeup();
    }

    /**
     * Pause polling from the underlying Kafka Broker.
     * <p>
     * Note: If the poll system is currently not in state {@link io.confluent.parallelconsumer.internal.State#running running},
     * calling this method will be a no-op.
     * </p>
     */
    public void pausePollingAndWorkRegistrationIfRunning() {
        if (this.state == State.running) {
            log.debug("Transitioning broker poll system to state paused.");
            this.state = State.paused;
        } else {
            log.debug("Skipping transition of broker poll system to state paused. Current state is {}.", this.state);
        }
    }

    /**
     * Resume polling from the underlying Kafka Broker.
     * <p>
     * Note: If the poll system is currently not in state {@link io.confluent.parallelconsumer.internal.State#paused paused},
     * calling this method will be a no-op.
     * </p>
     */
    public void resumePollingAndWorkRegistrationIfPaused() {
        if (this.state == State.paused) {
            log.debug("Transitioning broker poll system to state running.");
            this.state = State.running;
        } else {
            log.debug("Skipping transition of broker poll system to state running. Current state is {}.", this.state);
        }
    }
}
