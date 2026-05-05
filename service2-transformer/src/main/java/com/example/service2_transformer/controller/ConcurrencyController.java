package com.example.service2_transformer.controller;

import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class ConcurrencyController {
    private final KafkaListenerEndpointRegistry registry;
    @PostMapping("/listener/stop")
    public Map<String, Object> stopListener() {
        for (MessageListenerContainer container : registry.getListenerContainers()) {
            container.stop();
        }
        return Map.of("success", true, "message", "Listener stopped");
    }

    @PostMapping("/listener/start")
    public Map<String, Object> startListener() {
        for (MessageListenerContainer container : registry.getListenerContainers()) {
            container.start();
        }
        return Map.of("success", true, "message", "Listener started");
    }
    @PostMapping("/concurrency")
    public Map<String, Object> setConcurrency(@RequestParam int value) {
        if (value < 1 || value > 10) {
            return Map.of("success", false, "message", "Concurrency must be between 1 and 10");
        }

        for (MessageListenerContainer container : registry.getListenerContainers()) {
            container.stop();
            container.getContainerProperties().setGroupId("transformer-group");
            if (container instanceof ConcurrentMessageListenerContainer) {
                ((ConcurrentMessageListenerContainer<?, ?>) container)
                    .setConcurrency(value);
            }
            container.start();
        }

        return Map.of(
            "success", true,
            "concurrency", value,
            "message", "Listener restarted with concurrency " + value
        );
    }
}
