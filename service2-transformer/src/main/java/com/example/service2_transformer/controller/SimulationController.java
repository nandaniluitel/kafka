package com.example.service2_transformer.controller;

import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequiredArgsConstructor
public class SimulationController {

    private final KafkaListenerEndpointRegistry registry;


    @Value("${kafka.consumer.high.concurrency}")
    private int highConcurrency;

    @Value("${kafka.consumer.low.concurrency}")
    private int lowConcurrency;

    @PostMapping("/simulation/high/start")
    public Map<String, Object> startHigh() {
        return startSimulation("high-concurrency-listener", highConcurrency);
    }

    @PostMapping("/simulation/low/start")
    public Map<String, Object> startLow() {
        return startSimulation("low-concurrency-listener", lowConcurrency);
    }

    @PostMapping("/simulation/stop")
    public Map<String, Object> stopAll() {
        registry.getListenerContainers().forEach(c -> {
            if (c.isRunning()) c.stop();
        });
        return Map.of("success", true, "message", "All listeners stopped");
    }

    @GetMapping("/simulation/status")
    public Map<String, Object> status() {
        MessageListenerContainer high = registry.getListenerContainer("high-concurrency-listener");
        MessageListenerContainer low = registry.getListenerContainer("low-concurrency-listener");
        return Map.of(
            "high-concurrency-listener", Map.of("running", high != null && high.isRunning()),
            "low-concurrency-listener", Map.of("running", low != null && low.isRunning())
        );
    }

    private Map<String, Object> startSimulation(String listenerId, int concurrency) {
        try {
            MessageListenerContainer container = registry.getListenerContainer(listenerId);
            if (container == null) {
                return Map.of("success", false, "message", "Listener not found: " + listenerId);
            }
            if (container.isRunning()) {
                container.stop();
                Thread.sleep(2000);
            }
            container.start();
            log.info("Started {} with concurrency {}", listenerId, concurrency);
            return Map.of("success", true, "listenerId", listenerId, "concurrency", concurrency);
        } catch (Exception e) {
            log.error("Failed to start {}: {}", listenerId, e.getMessage());
            return Map.of("success", false, "message", e.getMessage());
        }
    }
}
