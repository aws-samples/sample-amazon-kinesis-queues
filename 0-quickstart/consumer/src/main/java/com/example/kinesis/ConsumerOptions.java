package com.example.kinesis;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

record ConsumerOptions(
        String streamName,
        String region,
        String applicationName) {

    static final String USAGE = """
            Usage: java -jar consumer/target/kinesis-consumer.jar \\
              --stream-name YOUR_STREAM_NAME --region YOUR_REGION \\
              --application-name YOUR_KCL_TABLE_NAME

            CDK supplies these settings and creates the DynamoDB checkpoint table.
            The consumer uses its ECS task role and runs until stopped. It starts at
            TRIM_HORIZON unless a saved checkpoint exists.
            """;

    static ConsumerOptions parse(String[] args) {
        Set<String> allowed = Set.of("--stream-name", "--region", "--application-name");
        Map<String, String> values = new HashMap<>();
        for (int i = 0; i < args.length; i += 2) {
            String key = args[i];
            if (!allowed.contains(key)) {
                throw new IllegalArgumentException("Unknown option: " + key);
            }
            if (i + 1 == args.length || args[i + 1].isBlank() || args[i + 1].startsWith("--")) {
                throw new IllegalArgumentException("Missing value for " + key);
            }
            if (values.putIfAbsent(key, args[i + 1]) != null) {
                throw new IllegalArgumentException("Duplicate option: " + key);
            }
        }
        String stream = required(values, "--stream-name");
        String region = required(values, "--region");
        String application = required(values, "--application-name");
        if (!stream.matches("[a-zA-Z0-9_.-]{1,128}")) {
            throw new IllegalArgumentException("Stream name must contain 1–128 letters, numbers, _, . or -.");
        }
        if (!region.matches("[a-z]{2}(?:-[a-z]+)+-\\d+")) {
            throw new IllegalArgumentException("Supply an AWS Region name, for example us-east-1.");
        }
        // Leave room for KCL's legacy table-name suffixes, even for a new single-table app.
        if (!application.matches("[a-zA-Z0-9_.-]{3,200}")) {
            throw new IllegalArgumentException("Application name must contain 3–200 letters, numbers, _, . or -.");
        }
        return new ConsumerOptions(stream, region, application);
    }

    private static String required(Map<String, String> values, String key) {
        String value = values.get(key);
        if (value == null) {
            throw new IllegalArgumentException("Required option: " + key);
        }
        return value;
    }
}
