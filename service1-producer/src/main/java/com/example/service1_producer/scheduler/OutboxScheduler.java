package com.example.service1_producer.scheduler;

import com.example.service1_producer.repository.OutboxRepository;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import lombok.extern.slf4j.Slf4j;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.apache.kafka.clients.producer.RecordMetadata;
@Component
@RequiredArgsConstructor

public class OutboxScheduler {
    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(OutboxScheduler.class);
    @Value("${kafka.topic}")
    private String topic;

    @Scheduled(fixedDelayString = "${outbox.scheduler.delay-ms:3000}")
    public void processOutbox() {
        List<Map<String, Object>> pendingRecords = outboxRepository.findPendingOutbox();

        if (pendingRecords.isEmpty()) {
            return;
        }

        log.info("Outbox scheduler picked up {} PENDING records", pendingRecords.size());

        for (Map<String, Object> record : pendingRecords) {
            String id = (String) record.get("ID");
            String payload = (String) record.get("PAYLOAD");

            try {
                CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, id, payload);
                future.whenComplete((result, ex) -> {
                    if (ex == null) {
                        int partition = result.getRecordMetadata().partition();
                        long offset = result.getRecordMetadata().offset();
                        log.info("Published id={} to Kafka partition={} offset={} | outbox status -> PUBLISHED", id, partition, offset);
                        outboxRepository.markPublished(id);
                    } else {
                        log.error("Failed to publish id={} to Kafka: {}", id, ex.getMessage());
                        outboxRepository.markFailed(id);
                    }
                });
            } catch (Exception e) {
                log.error("Exception publishing id={}: {}", id, e.getMessage());
                outboxRepository.markFailed(id);
            }
        }
    }
}
