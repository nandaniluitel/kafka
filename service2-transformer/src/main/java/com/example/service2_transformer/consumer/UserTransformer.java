package com.example.service2_transformer.consumer;

import com.example.service2_transformer.model.User;
import com.example.service2_transformer.repository.StatusMappingRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class UserTransformer {
    private final StatusMappingRepository statusMappingRepository;
    private final ObjectMapper objectMapper;

    @Value("${service3.url}")
    private String service3Url;

    @Value("${kafka.consumer.concurrency}")
    private int concurrency;

    private final HttpClient httpClient = HttpClient.newHttpClient();

    @KafkaListener(
        topics = "${kafka.topic}",
        groupId = "${spring.kafka.consumer.group-id}",
        concurrency = "${kafka.consumer.concurrency}"
    )
    public void consume(ConsumerRecord<String,String> record){
        try {
            String payload = record.value();
            int partition = record.partition();
            long offset = record.offset();

            User user = objectMapper.readValue(payload, User.class);
            String rawStatus = user.getStatus();
            String mappedStatus = statusMappingRepository.getMappedStatus(rawStatus);
            user.setStatus(mappedStatus);

            log.info("Transformed id={} status: {} -> {} | partition={} offset={}",
                user.getId(), rawStatus, mappedStatus, partition, offset);

            String transformedPayload = objectMapper.writeValueAsString(user);
            sendToService3(user.getId(), transformedPayload);

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

            HttpResponse<String> response = httpClient.send(request,
                HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200 || response.statusCode() == 201) {
                log.info("Successfully forwarded id={} to Service 3", id);
            } else {
                log.error("[REPUBLISH NEEDED] Service 3 returned {} for id={}. To retry: POST http://localhost:8081/outbox/republish?id={}",
                    response.statusCode(), id, id);
            }
        } catch (Exception e) {
            log.error("[REPUBLISH NEEDED] Could not reach Service 3 for id={}. To retry: POST http://localhost:8081/outbox/republish?id={}",
                id, id);
        }
    }

}
