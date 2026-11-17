package com.example.fulfillment;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.KinesisClientUtil;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.coordinator.WorkerStateChangeListener;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.processor.SingleStreamTracker;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

/**
 * Starts the KCL scheduler for the order fulfillment consumer. Startup and shutdown
 * follow the Quickstart consumer (0-quickstart/consumer); the record processor is
 * FulfillmentProcessor.
 *
 * Every task runs the same code with the same application name, so the tasks share
 * one lease table and KCL spreads the stream's shards across them. A task without a
 * lease has nothing to do.
 */
public final class KclFulfillmentApp {
    private KclFulfillmentApp() {}

    public static void main(String[] args) {
        int status;
        try {
            status = run(Settings.fromEnvironment());
        } catch (IllegalArgumentException e) {
            JsonLog.error("INVALID_SETTINGS", JsonLog.fields("error", e.getMessage()));
            status = 2;
        } catch (Exception e) {
            JsonLog.error("CRASHED", JsonLog.fields("error", JsonLog.errorMessage(e)));
            status = 1;
        }
        System.exit(status);
    }

    static int run(Settings settings) throws Exception {
        CountDownLatch finished = new CountDownLatch(1);
        var credentials = DefaultCredentialsProvider.builder().build();
        Region region = Region.of(settings.region());
        var timeouts = ClientOverrideConfiguration.builder()
                .apiCallTimeout(Duration.ofSeconds(45))
                .apiCallAttemptTimeout(Duration.ofSeconds(20))
                .build();
        ScheduledExecutorService statsTimer = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "stats");
            thread.setDaemon(true);
            return thread;
        });
        try (var kinesis = KinesisClientUtil.createKinesisAsyncClient(KinesisAsyncClient.builder()
                     .region(region).credentialsProvider(credentials).overrideConfiguration(timeouts));
             var dynamodb = DynamoDbAsyncClient.builder()
                     .region(region).credentialsProvider(credentials).overrideConfiguration(timeouts).build();
             var cloudwatch = CloudWatchAsyncClient.builder()
                     .region(region).credentialsProvider(credentials).overrideConfiguration(timeouts).build();
             var sqs = SqsAsyncClient.builder()
                     .region(region).credentialsProvider(credentials).overrideConfiguration(timeouts).build()) {
            // Catch a wrong stream name or Region before starting KCL.
            kinesis.describeStreamSummary(request -> request.streamName(settings.streamName())).join();

            CountDownLatch stop = new CountDownLatch(1);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Consumer<Throwable> onFailure = error -> {
                failure.compareAndSet(null, error);
                stop.countDown();
            };
            Stats stats = new Stats(settings.applicationName());
            OrderFulfillment fulfillment = new OrderFulfillment(dynamodb, settings);
            Sideline sideline = new Sideline(sqs, settings.sidelineQueueUrl());

            var streamTracker = new SingleStreamTracker(settings.streamName(),
                    InitialPositionInStreamExtended.newInitialPosition(settings.initialPosition()));
            // The worker ID is the task ID from the logs, so lease owners map to tasks.
            var configs = new ConfigsBuilder(streamTracker, settings.applicationName(),
                    kinesis, dynamodb, cloudwatch, JsonLog.TASK_ID,
                    () -> new FulfillmentProcessor(settings, fulfillment, sideline, stats, onFailure));
            var coordinator = configs.coordinatorConfig().workerStateChangeListener(new WorkerStateChangeListener() {
                @Override
                public void onWorkerStateChange(WorkerState state) {
                    JsonLog.info("KCL_WORKER_STATE", JsonLog.fields("state", String.valueOf(state)));
                }

                @Override
                public void onAllInitializationAttemptsFailed(Throwable error) {
                    onFailure.accept(error);
                }
            });
            // Small batches: a lease that moves mid-batch replays at most this many records.
            var retrieval = configs.retrievalConfig().retrievalSpecificConfig(
                    new PollingConfig(settings.streamName(), kinesis).maxRecords(settings.maxRecords()));
            var leases = configs.leaseManagementConfig();
            // Same as the Quickstart: balance leases on throughput, not on host CPU metrics.
            leases.workerUtilizationAwareAssignmentConfig().disableWorkerMetrics(true);
            // KCL's own per-shard metrics, such as MillisBehindLatest. SUMMARY keeps the
            // number of custom metrics down; the default, DETAILED, publishes many more.
            var metrics = configs.metricsConfig().metricsLevel(MetricsLevel.SUMMARY);
            Scheduler scheduler = new Scheduler(configs.checkpointConfig(), coordinator,
                    leases, configs.lifecycleConfig(), metrics, configs.processorConfig(), retrieval);

            Thread worker = new Thread(() -> {
                try {
                    scheduler.run();
                } catch (Throwable error) {
                    onFailure.accept(error);
                } finally {
                    if (stop.getCount() != 0) {
                        onFailure.accept(new IllegalStateException("KCL worker stopped unexpectedly."));
                    }
                    stop.countDown();
                }
            }, "kcl-worker");
            worker.setDaemon(true);
            // ECS sends SIGTERM on scale-in and deployments, then SIGKILL after the stop timeout.
            Thread shutdownHook = new Thread(() -> {
                stop.countDown();
                try {
                    finished.await(60, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "consumer-shutdown");
            Runtime.getRuntime().addShutdownHook(shutdownHook);
            try {
                JsonLog.info("STARTED", JsonLog.fields(
                        "stream", settings.streamName(), "application", settings.applicationName(),
                        "kcl", "3.5.4", "initialPosition", settings.initialPosition().toString(),
                        "maxAttempts", settings.maxAttempts(), "maxRecords", settings.maxRecords()));
                statsTimer.scheduleAtFixedRate(stats::report, settings.statsIntervalSeconds(),
                        settings.statsIntervalSeconds(), TimeUnit.SECONDS);
                worker.start();
                stop.await();
            } finally {
                stop.countDown();
                stopScheduler(scheduler, worker, onFailure, Duration.ofSeconds(40), Duration.ofSeconds(5));
                statsTimer.shutdownNow();
                stats.report();
                try {
                    Runtime.getRuntime().removeShutdownHook(shutdownHook);
                } catch (IllegalStateException ignored) {
                    // The JVM has already begun running this hook after a stop signal.
                }
            }
            if (failure.get() != null) {
                JsonLog.error("STOPPED_AFTER_FAILURE", JsonLog.fields(
                        "error", JsonLog.errorMessage(failure.get()),
                        "note", "a restart resumes from the last checkpoint"));
                return 1;
            }
            JsonLog.info("STOPPED", JsonLog.fields());
            return 0;
        } finally {
            try {
                credentials.close();
            } finally {
                finished.countDown();
            }
        }
    }

    static void stopScheduler(Scheduler scheduler, Thread worker, Consumer<Throwable> onFailure,
            Duration gracefulTimeout, Duration joinTimeout) {
        JsonLog.info("STOPPING", JsonLog.fields("note", "checkpointing completed work"));
        try {
            if (!scheduler.startGracefulShutdown().get(gracefulTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new IOException("KCL could not complete graceful shutdown.");
            }
            worker.join(joinTimeout.toMillis());
            if (worker.isAlive()) {
                throw new IOException("KCL worker did not stop.");
            }
        } catch (Exception e) {
            onFailure.accept(e);
            // KCL may hold its initialization lock while a DynamoDB index is being created.
            // shutdown() acquires that lock, so run the fallback off the main thread.
            Thread fallback = new Thread(scheduler::shutdown, "kcl-forced-shutdown");
            fallback.setDaemon(true);
            fallback.start();
            try {
                worker.join(joinTimeout.toMillis());
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
