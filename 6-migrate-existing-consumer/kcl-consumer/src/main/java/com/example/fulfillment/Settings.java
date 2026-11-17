package com.example.fulfillment;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import software.amazon.kinesis.common.InitialPositionInStream;

/** Runtime settings, read from environment variables that the CDK stack sets. */
record Settings(
        String region,
        String streamName,
        String applicationName,
        String ordersTable,
        String sidelineQueueUrl,
        /** Attempts per order on this task before it gives up and sidelines the order. */
        int maxAttempts,
        /** Records per GetRecords call. Smaller batches bound the replay after a lease moves. */
        int maxRecords,
        int processingMsMin,
        int processingMsMax,
        /** Products the warehouse no longer stocks: orders for them fail on every attempt. */
        Set<String> discontinuedSkus,
        int statsIntervalSeconds,
        InitialPositionInStream initialPosition) {

    static Settings fromEnvironment() {
        int min = number("PROCESSING_MS_MIN", 300, 0, 60_000);
        int max = number("PROCESSING_MS_MAX", 800, 0, 60_000);
        if (max < min) {
            throw new IllegalArgumentException("PROCESSING_MS_MAX must be >= PROCESSING_MS_MIN.");
        }
        String position = optional("INITIAL_POSITION", "LATEST").toUpperCase();
        if (!position.equals("LATEST") && !position.equals("TRIM_HORIZON")) {
            throw new IllegalArgumentException("INITIAL_POSITION must be LATEST or TRIM_HORIZON.");
        }
        return new Settings(
                required("AWS_REGION"),
                required("STREAM_NAME"),
                required("APPLICATION_NAME"),
                required("ORDERS_TABLE"),
                required("SIDELINE_QUEUE_URL"),
                number("MAX_ATTEMPTS", 3, 1, 10),
                number("MAX_RECORDS", 25, 1, 10_000),
                min,
                max,
                Arrays.stream(optional("DISCONTINUED_SKUS", "SKU-DISCONTINUED").split(","))
                        .map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toUnmodifiableSet()),
                number("STATS_INTERVAL_SECONDS", 10, 5, 3600),
                InitialPositionInStream.valueOf(position));
    }

    private static String required(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Environment variable " + name + " is required.");
        }
        return value.trim();
    }

    private static String optional(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value.trim();
    }

    private static int number(String name, int fallback, int min, int max) {
        String text = optional(name, String.valueOf(fallback));
        try {
            int value = Integer.parseInt(text);
            if (value < min || value > max) {
                throw new NumberFormatException();
            }
            return value;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be a whole number from " + min + " to " + max + ", got \"" + text + "\".");
        }
    }
}
