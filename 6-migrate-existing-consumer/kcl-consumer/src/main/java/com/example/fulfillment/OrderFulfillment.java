package com.example.fulfillment;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadLocalRandom;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;

/**
 * Fulfills each order at most once: the business logic the migration keeps. The queue
 * consumer (worker/src/fulfillment.ts) has the same rules and uses the same orders
 * table, so both consumers can run side by side during the cutover without shipping
 * an order twice.
 *
 * - A consistent GetItem skips orders that are already done: a replay after a lease
 *   moved, a producer retry, or an order the other consumer shipped first.
 * - A conditional put on orderId records the fulfillment. When both consumers work on
 *   the same order at the same time, only the first put succeeds.
 *
 * The warehouse call runs before the conditional put, so in that race it runs twice.
 * A real warehouse API should take orderId as its idempotency key for that reason.
 */
final class OrderFulfillment {
    /** The warehouse can't take this order right now, or ever. The consumer can't tell which. */
    static final class WarehouseException extends Exception {
        WarehouseException(String message) {
            super(message);
        }
    }

    sealed interface Outcome permits Fulfilled, Duplicate {}

    record Fulfilled(String shipmentId, long processingMs) implements Outcome {}

    /**
     * Already fulfilled. skipped: the order was in the table, so the warehouse wasn't
     * called. Otherwise another delivery finished first while this one was working.
     */
    record Duplicate(boolean skipped) implements Outcome {}

    private final DynamoDbAsyncClient dynamo;
    private final Settings settings;

    OrderFulfillment(DynamoDbAsyncClient dynamo, Settings settings) {
        this.dynamo = dynamo;
        this.settings = settings;
    }

    Outcome fulfill(Orders.Order order, int attempt) throws WarehouseException {
        var existing = dynamo.getItem(request -> request
                .tableName(settings.ordersTable())
                .key(Map.of("orderId", s(order.orderId())))
                .projectionExpression("orderId")
                .consistentRead(true)).join();
        if (existing.hasItem() && !existing.item().isEmpty()) {
            countDuplicate(order.orderId(), "duplicatesSkipped");
            return new Duplicate(true);
        }

        long started = System.currentTimeMillis();
        String shipmentId = bookWarehouse(order);
        long processingMs = System.currentTimeMillis() - started;
        try {
            dynamo.putItem(request -> request
                    .tableName(settings.ordersTable())
                    .item(Map.ofEntries(
                            Map.entry("orderId", s(order.orderId())),
                            Map.entry("customerId", s(order.customerId())),
                            Map.entry("shipmentId", s(shipmentId)),
                            Map.entry("total", n(order.total())),
                            Map.entry("currency", s(order.currency())),
                            Map.entry("orderCreatedAt", s(order.createdAt())),
                            Map.entry("fulfilledAt", s(Instant.now().toString())),
                            Map.entry("fulfilledBy", s(JsonLog.TASK_ID)),
                            Map.entry("consumer", s("kcl")),
                            Map.entry("attempt", n(attempt)),
                            Map.entry("processingMs", n(processingMs))))
                    .conditionExpression("attribute_not_exists(orderId)")).join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof ConditionalCheckFailedException) {
                countDuplicate(order.orderId(), "duplicatesDiscarded");
                return new Duplicate(false);
            }
            throw e;
        }
        return new Fulfilled(shipmentId, processingMs);
    }

    /**
     * Stand-in for reserving stock and booking a shipment with an external warehouse API.
     * An order for a product the warehouse no longer stocks fails on every attempt.
     */
    private String bookWarehouse(Orders.Order order) throws WarehouseException {
        int min = settings.processingMsMin();
        int max = settings.processingMsMax();
        sleep(max > min ? ThreadLocalRandom.current().nextInt(min, max + 1) : min);
        for (Orders.Item item : order.items()) {
            if (settings.discontinuedSkus().contains(item.sku())) {
                throw new WarehouseException("warehouse rejected " + order.orderId() + ": " + item.sku() + " is no longer stocked");
            }
        }
        return "shp-" + UUID.randomUUID().toString().substring(0, 12);
    }

    /** Demo bookkeeping, so the status script can show how many duplicates the table absorbed. */
    private void countDuplicate(String orderId, String counter) {
        try {
            dynamo.updateItem(request -> request
                    .tableName(settings.ordersTable())
                    .key(Map.of("orderId", s(orderId)))
                    .updateExpression("ADD #counter :one SET lastDuplicateAt = :now, lastDuplicateBy = :by")
                    .conditionExpression("attribute_exists(orderId)")
                    .expressionAttributeNames(Map.of("#counter", counter))
                    .expressionAttributeValues(Map.of(
                            ":one", n(1),
                            ":now", s(Instant.now().toString()),
                            ":by", s("kcl:" + JsonLog.TASK_ID)))).join();
        } catch (RuntimeException ignored) {
            // Best effort: a failed counter mustn't fail the record.
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while calling the warehouse", e);
        }
    }

    private static AttributeValue s(String value) {
        return AttributeValue.fromS(value == null ? "" : value);
    }

    private static AttributeValue n(Number value) {
        return AttributeValue.fromN(String.valueOf(value));
    }
}
