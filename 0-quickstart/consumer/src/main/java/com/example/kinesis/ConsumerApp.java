package com.example.kinesis;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.KinesisClientUtil;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.coordinator.WorkerStateChangeListener;
import software.amazon.kinesis.processor.SingleStreamTracker;
import software.amazon.kinesis.retrieval.polling.PollingConfig;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

public final class ConsumerApp {
    private ConsumerApp() {}

    public static void main(String[] args) {
        if (args.length == 1 && "--help".equals(args[0])) {
            System.out.print(ConsumerOptions.USAGE);
            return;
        }
        int status;
        try {
            status = run(ConsumerOptions.parse(args));
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            System.err.print(ConsumerOptions.USAGE);
            status = 2;
        } catch (Exception e) {
            System.err.println("Consumer failed: " + e);
            status = 1;
        }
        System.exit(status);
    }

    static int run(ConsumerOptions options) throws Exception {
        CountDownLatch finished = new CountDownLatch(1);
        var credentials = DefaultCredentialsProvider.builder().build();
        Region region = Region.of(options.region());
        var timeouts = ClientOverrideConfiguration.builder()
                .apiCallTimeout(Duration.ofSeconds(45))
                .apiCallAttemptTimeout(Duration.ofSeconds(20))
                .build();
        try (var kinesis = KinesisClientUtil.createKinesisAsyncClient(KinesisAsyncClient.builder()
                     .region(region).credentialsProvider(credentials).overrideConfiguration(timeouts));
             var dynamodb = DynamoDbAsyncClient.builder()
                     .region(region).credentialsProvider(credentials).overrideConfiguration(timeouts).build();
             var cloudwatch = CloudWatchAsyncClient.builder()
                     .region(region).credentialsProvider(credentials).overrideConfiguration(timeouts).build()) {
            // Catch a wrong stream name or Region before starting KCL.
            kinesis.describeStreamSummary(request -> request.streamName(options.streamName())).join();

            CountDownLatch stop = new CountDownLatch(1);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Consumer<Throwable> onFailure = error -> {
                failure.compareAndSet(null, error);
                stop.countDown();
            };
            var streamTracker = new SingleStreamTracker(options.streamName(),
                    InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON));
            var configs = new ConfigsBuilder(streamTracker, options.applicationName(),
                    kinesis, dynamodb, cloudwatch, UUID.randomUUID().toString(),
                    () -> new PrintingRecordProcessor(line -> {
                        System.out.println(line);
                        if (System.out.checkError()) {
                            throw new IllegalStateException("Unable to write record to standard output.");
                        }
                    }, onFailure));
            var coordinator = configs.coordinatorConfig().workerStateChangeListener(new WorkerStateChangeListener() {
                @Override
                public void onWorkerStateChange(WorkerState state) {
                    System.err.println("KCL worker: " + state);
                }

                @Override
                public void onAllInitializationAttemptsFailed(Throwable error) {
                    onFailure.accept(error);
                }
            });
            var retrieval = configs.retrievalConfig()
                    .retrievalSpecificConfig(new PollingConfig(options.streamName(), kinesis));
            var leases = configs.leaseManagementConfig();
            // This single-worker demo does not need host CPU metrics for load balancing.
            leases.workerUtilizationAwareAssignmentConfig().disableWorkerMetrics(true);
            Scheduler scheduler = new Scheduler(configs.checkpointConfig(), coordinator,
                    leases, configs.lifecycleConfig(), configs.metricsConfig(),
                    configs.processorConfig(), retrieval);
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
            // A stalled SDK/KCL worker must not keep the process alive after bounded shutdown.
            worker.setDaemon(true);
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
                System.err.printf("Consuming %s in %s using KCL 3.5.4 (polling).%n",
                        options.streamName(), options.region());
                System.err.printf("Application/table: %s. Initial position: TRIM_HORIZON; saved checkpoints take precedence.%n",
                        options.applicationName());
                worker.start();
                stop.await();
            } finally {
                stop.countDown();
                stopScheduler(scheduler, worker, onFailure, Duration.ofSeconds(40), Duration.ofSeconds(5));
                try {
                    Runtime.getRuntime().removeShutdownHook(shutdownHook);
                } catch (IllegalStateException ignored) {
                    // The JVM has already begun running this hook after a stop signal.
                }
            }
            if (failure.get() != null) {
                System.err.println("Consumer failed; restart with the same application name to resume: " + failure.get());
                return 1;
            }
            System.err.println("Consumer stopped.");
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
        System.err.println("Stopping consumer and checkpointing completed work...");
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
