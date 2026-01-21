/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.demos.txbuffering;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class TestContext implements AutoCloseable {

    private final ExecutorService executor;
    private final KafkaConsumer<String, String> consumer;
    private final Map<String, List<ConsumerRecord<String, String>>> buffers;
    private final Set<String> subscribedTopics;
    private final AtomicReference<Throwable> flinkError;
    private final AtomicBoolean closed;

    public TestContext(String bootstrapServers) {
        this.executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "flink-job");
            t.setDaemon(true);
            return t;
        });
        this.buffers = new HashMap<>();
        this.subscribedTopics = new HashSet<>();
        this.flinkError = new AtomicReference<>();
        this.closed = new AtomicBoolean(false);

        this.consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + System.currentTimeMillis(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()
        ));

        // Start Flink job
        executor.submit(() -> {
            try {
                DataStreamV2Job.run(bootstrapServers);
            }
            catch (Throwable e) {
                if (!(e instanceof InterruptedException)) {
                    flinkError.set(e);
                }
            }
        });
    }

    public Future<ConsumerRecord<String, String>> takeOne(String topic) {
        CompletableFuture<ConsumerRecord<String, String>> future = new CompletableFuture<>();
        take(topic, 1).thenAccept(list -> future.complete(list.getFirst()))
                .exceptionally(e -> {
                    future.completeExceptionally(e);
                    return null;
                });
        return future;
    }

    public CompletableFuture<List<ConsumerRecord<String, String>>> take(String topic, int n) {
        if (subscribedTopics.add(topic)) {
            consumer.subscribe(List.of(topic));
        }

        return CompletableFuture.supplyAsync(() -> {
            List<ConsumerRecord<String, String>> result = new ArrayList<>(n);
            List<ConsumerRecord<String, String>> buffer = buffers.computeIfAbsent(topic, k -> new ArrayList<>());

            // First, drain from buffer
            while (!buffer.isEmpty() && result.size() < n) {
                result.add(buffer.removeFirst());
            }

            // Poll for more if needed
            while (result.size() < n && !closed.get()) {
                Throwable error = flinkError.get();
                if (error != null) {
                    throw new RuntimeException("Flink job failed", error);
                }

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    if (result.size() < n) {
                        result.add(record);
                    }
                    else {
                        buffer.add(record);
                    }
                }
            }

            if (result.size() < n) {
                throw new RuntimeException("Context closed before collecting " + n + " records");
            }
            return result;
        });
    }

    public void assertDrained() {
        for (Map.Entry<String, List<ConsumerRecord<String, String>>> entry : buffers.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                throw new AssertionError("Buffer for topic '" + entry.getKey() + "' is not empty, contains "
                        + entry.getValue().size() + " record(s)");
            }
        }
    }

    @Override
    public void close() {
        closed.set(true);
        executor.shutdownNow();
        consumer.close();
    }
}
