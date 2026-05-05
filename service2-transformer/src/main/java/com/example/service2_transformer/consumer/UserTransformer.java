package com.example.service2_transformer.consumer;

import com.example.service2_transformer.model.User;
import com.example.service2_transformer.repository.StatusMappingRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class UserTransformer implements ConsumerSeekAware {

    private final StatusMappingRepository statusMappingRepository;
    private final ObjectMapper objectMapper;

    @Value("${service3.url}")
    private String service3Url;

    private final HttpClient httpClient = HttpClient.newHttpClient();

    @KafkaListener(
        id = "high-concurrency-listener",
        topics = "${kafka.topic}",
        groupId = "${kafka.consumer.high.group-id}",
        concurrency = "${kafka.consumer.high.concurrency}",
        autoStartup = "false"
    )
    public void consumeHigh(ConsumerRecord<String, String> record) {
        processRecord(record);
    }

    @KafkaListener(
        id = "low-concurrency-listener",
        topics = "${kafka.topic}",
        groupId = "${kafka.consumer.low.group-id}",
        concurrency = "${kafka.consumer.low.concurrency}",
        autoStartup = "false"
    )
    public void consumeLow(ConsumerRecord<String, String> record) {
        processRecord(record);
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        assignments.keySet().forEach(tp -> callback.seekToBeginning(tp.topic(), tp.partition()));
        log.info("Seeking {} partitions to beginning", assignments.size());
    }

    private void processRecord(ConsumerRecord<String, String> record) {
        try {
            User user = objectMapper.readValue(record.value(), User.class);
            String rawStatus = user.getStatus();
            String mappedStatus = statusMappingRepository.getMappedStatus(rawStatus);
            user.setStatus(mappedStatus);

            log.debug("Transformed id={} {} -> {} | partition={} offset={}",
                user.getId(), rawStatus, mappedStatus, record.partition(), record.offset());

            sendToService3(user.getId(), objectMapper.writeValueAsString(user));
        } catch (Exception e) {
            log.error("Error processing record: {}", e.getMessage());
        }
    }

    private void sendToService3(String id, String payload) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(service3Url + "/users"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200 && response.statusCode() != 201) {
                log.error("Service 3 returned {} for id={}", response.statusCode(), id);
            }
        } catch (Exception e) {
            log.error("Could not reach Service 3 for id={}: {}", id, e.getMessage());
        }
    }
}
