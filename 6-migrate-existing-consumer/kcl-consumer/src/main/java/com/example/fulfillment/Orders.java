package com.example.fulfillment;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Parses the OrderPlaced events the checkout writes to the stream. The same rules as
 * shared/src/orders.ts, which the queue worker uses.
 */
final class Orders {
    private Orders() {}

    record Item(String sku, int qty, double price) {}

    record Order(String orderId, String customerId, List<Item> items, double total, String currency, String createdAt) {}

    enum Kind {
        ORDER,
        /** Valid JSON with a different eventType: not this consumer's business. */
        OTHER,
        /** Can never succeed, however often it is retried. */
        INVALID
    }

    record Parsed(Kind kind, Order order, String detail) {
        static Parsed other(String eventType) {
            return new Parsed(Kind.OTHER, null, eventType);
        }

        static Parsed invalid(String reason) {
            return new Parsed(Kind.INVALID, null, reason);
        }
    }

    static Parsed parse(ByteBuffer data) {
        String text;
        try {
            text = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(data.asReadOnlyBuffer()).toString();
        } catch (CharacterCodingException e) {
            return Parsed.invalid("not valid UTF-8");
        }
        JsonNode event;
        try {
            event = JsonLog.MAPPER.readTree(text);
        } catch (Exception e) {
            return Parsed.invalid("not valid JSON");
        }
        if (event == null || !event.isObject()) {
            return Parsed.invalid("not a JSON object");
        }
        if (!"OrderPlaced".equals(event.path("eventType").asText(null))) {
            return Parsed.other(event.path("eventType").asText("unknown"));
        }
        String orderId = event.path("orderId").asText("");
        String customerId = event.path("customerId").asText("");
        if (!event.path("orderId").isTextual() || orderId.isEmpty()) {
            return Parsed.invalid("missing orderId");
        }
        if (!event.path("customerId").isTextual() || customerId.isEmpty()) {
            return Parsed.invalid("missing customerId");
        }
        JsonNode itemsNode = event.path("items");
        if (!itemsNode.isArray() || itemsNode.isEmpty()) {
            return Parsed.invalid("no items");
        }
        List<Item> items = new ArrayList<>();
        for (JsonNode item : itemsNode) {
            if (!item.path("sku").isTextual() || !item.path("qty").isIntegralNumber()
                    || item.path("qty").asInt() <= 0 || !item.path("price").isNumber()) {
                return Parsed.invalid("malformed item");
            }
            items.add(new Item(item.path("sku").asText(), item.path("qty").asInt(), item.path("price").asDouble()));
        }
        if (!event.path("total").isNumber()) {
            return Parsed.invalid("missing total");
        }
        return new Parsed(Kind.ORDER, new Order(orderId, customerId, List.copyOf(items),
                event.path("total").asDouble(), event.path("currency").asText("EUR"),
                event.path("createdAt").asText("")), null);
    }
}
