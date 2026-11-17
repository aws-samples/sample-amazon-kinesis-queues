package com.example.fulfillment;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.PrintStream;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * One JSON object per line, in the same shape as the queue worker's logs, so both
 * can be queried the same way in CloudWatch Logs Insights and by scripts/status.ts.
 */
final class JsonLog {
    /** Short ID for this task process. It's also the KCL worker ID, so lease owners map to tasks. */
    static final String TASK_ID = UUID.randomUUID().toString().substring(0, 8);
    static final ObjectMapper MAPPER = new ObjectMapper();

    private JsonLog() {}

    /** Builds a field map from name/value pairs, skipping null values. */
    static Map<String, Object> fields(Object... namesAndValues) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i + 1 < namesAndValues.length; i += 2) {
            if (namesAndValues[i + 1] != null) {
                map.put(String.valueOf(namesAndValues[i]), namesAndValues[i + 1]);
            }
        }
        return map;
    }

    static void info(String event, Map<String, Object> fields) {
        write("INFO", event, fields);
    }

    static void warn(String event, Map<String, Object> fields) {
        write("WARN", event, fields);
    }

    static void error(String event, Map<String, Object> fields) {
        write("ERROR", event, fields);
    }

    static String errorMessage(Throwable error) {
        Throwable cause = error;
        while ((cause instanceof java.util.concurrent.CompletionException
                || cause instanceof java.util.concurrent.ExecutionException) && cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause.getClass().getSimpleName() + ": " + cause.getMessage();
    }

    private static synchronized void write(String level, String event, Map<String, Object> fields) {
        ObjectNode line = MAPPER.createObjectNode();
        line.put("time", Instant.now().toString());
        line.put("level", level);
        line.put("task", TASK_ID);
        line.put("consumer", "kcl");
        line.put("event", event);
        line.setAll((ObjectNode) MAPPER.valueToTree(fields));
        PrintStream out = "INFO".equals(level) ? System.out : System.err;
        try {
            out.println(MAPPER.writeValueAsString(line));
        } catch (JsonProcessingException e) {
            out.println("{\"level\":\"ERROR\",\"event\":\"LOG_FAILED\",\"error\":\"" + e.getOriginalMessage() + "\"}");
        }
    }
}
