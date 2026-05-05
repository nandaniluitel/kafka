package com.example.service1_producer.service;

import com.example.service1_producer.model.User;
import com.example.service1_producer.repository.OutboxRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class DataInitializerService {

    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaBootstrapServers;

    @Value("${kafka.topic}")
    private String kafkaTopic;

    private static final int SEED_COUNT = 30_000;

    private static final List<String> NAMES = List.of(
            "Alice Johnson", "Bob Smith", "Charlie Brown",
            "Diana Prince", "Edward Norton", "Fiona Green"
    );
    private static final List<String> STATUSES = List.of("1", "2", "3");

    @PostConstruct
    public void init() {
        resetKafkaTopic();
        resetDatabase();
        seedData();
    }

    private void resetKafkaTopic() {
        log.info("Resetting Kafka topic: {}", kafkaTopic);
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaBootstrapServers);

        try (AdminClient admin = AdminClient.create(props)) {
            admin.deleteTopics(List.of(kafkaTopic)).all().get();
            log.info("Kafka topic '{}' deleted", kafkaTopic);
            Thread.sleep(2000);

            NewTopic newTopic = new NewTopic(kafkaTopic, 12, (short) 1);
            admin.createTopics(List.of(newTopic)).all().get();
            log.info("Kafka topic '{}' recreated with 12 partitions", kafkaTopic);
        } catch (Exception e) {
            // Topic may not exist on first run — safe to continue
            log.warn("Kafka topic reset skipped or partial: {}", e.getMessage());
        }
    }

    private void resetDatabase() {
        log.info("Clearing H2 database...");
        outboxRepository.deleteAll();
        log.info("H2 database cleared");
    }

    private static final int BATCH_SIZE = 500;

    private void seedData() {
        log.info("Seeding {} records in batches of {}...", SEED_COUNT, BATCH_SIZE);

        List<User> userBatch = new ArrayList<>(BATCH_SIZE);
        List<String> payloadBatch = new ArrayList<>(BATCH_SIZE);

        for (int i = 0; i < SEED_COUNT; i++) {
            log.info("Seed count {}",i);
            User user = new User(
                    UUID.randomUUID().toString(),
                    NAMES.get((int) (Math.random() * NAMES.size())),
                    STATUSES.get((int) (Math.random() * STATUSES.size()))
            );
            try {
                userBatch.add(user);
                payloadBatch.add(objectMapper.writeValueAsString(user));
            } catch (Exception e) {
                log.error("Failed to serialize user at index {}: {}", i, e.getMessage());
            }

            if (userBatch.size() == BATCH_SIZE) {
                outboxRepository.insertUserBatch(userBatch);
                outboxRepository.insertOutboxBatch(userBatch, payloadBatch);
                userBatch.clear();
                payloadBatch.clear();
            }
        }

        // flush remaining
        if (!userBatch.isEmpty()) {
            outboxRepository.insertUserBatch(userBatch);
            outboxRepository.insertOutboxBatch(userBatch, payloadBatch);
        }

        log.info("Seeding complete — {} records queued into outbox", SEED_COUNT);
    }
}