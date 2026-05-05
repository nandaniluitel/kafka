package com.example.service1_producer.controller;

import com.example.service1_producer.model.User;
import com.example.service1_producer.repository.OutboxRepository;
import com.example.service1_producer.scheduler.OutboxScheduler;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import  com.fasterxml.jackson.core.ObjectCodec;
@RestController
@RequiredArgsConstructor
public class SeedController {
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(OutboxScheduler.class);
    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaBootstrapServers;

    @Value("${kafka.topic}")
    private String kafkaTopic;
    private static final List<String> NAMES = List.of(
        "Alice Johnson", "Bob Smith", "Charlie Brown",
        "Diana Prince", "Edward Norton", "Fiona Green"
    );
    private static final List<String> STATUSES = List.of("1", "2", "3");
    @PostMapping("/seed")
    public Map<String, Integer> seed(@RequestParam int count) throws Exception {
        for (int i = 0; i < count; i++) {
            User user = new User(
                UUID.randomUUID().toString(),
                NAMES.get((int)(Math.random() * NAMES.size())),
                STATUSES.get((int)(Math.random() * STATUSES.size()))
            );
            String payload = objectMapper.writeValueAsString(user);
            outboxRepository.insertUser(user);
            outboxRepository.insertOutbox(user,payload);
}
        log.info("\"Queued {} records into outbox\", count");
        return Map.of("queued", count);
    }
    @GetMapping("/outbox/stats")
public Map<String,Object> stats(){
        return outboxRepository.getStats();
    }
    @PostMapping("/outbox/republish")
    public Map<String, Object> republish(@RequestParam String id) {
        boolean reset = outboxRepository.republish(id);
        return Map.of(
            "id", id,
            "reset", reset,
            "message", reset
                ? "Record queued for republish"
                : "No outbox entry found for this id"
        );
    }
    @DeleteMapping("/reset")
    public ResponseEntity<Void> reset() {
        outboxRepository.deleteAll();
        return ResponseEntity.ok().build();
    }
    @DeleteMapping("/kafka/reset")
    public Map<String, Object> resetKafka() {
        try {
            Properties props = new Properties();
            props.put("bootstrap.servers", kafkaBootstrapServers);

            try (AdminClient admin = AdminClient.create(props)) {
                // delete topic
                admin.deleteTopics(List.of(kafkaTopic)).all().get();
                Thread.sleep(2000);

                // recreate with 3 partitions
                NewTopic newTopic = new NewTopic(kafkaTopic, 3, (short) 1);
                admin.createTopics(List.of(newTopic)).all().get();
            }
            return Map.of("success", true, "message", "Kafka topic reset — user-raw recreated with 3 partitions");
        } catch (Exception e) {
            return Map.of("success", false, "message", e.getMessage());
        }
    }
}

