package com.example.service1_producer.controller;

import com.example.service1_producer.repository.OutboxRepository;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class StatsController {

    private final OutboxRepository outboxRepository;

    @Value("${data.seed.count:30000}")
    private int seedCount;

    @GetMapping("/info")
    public Map<String, Object> info() {
        Map<String, Object> stats = outboxRepository.getStats();
        long published = ((Number) stats.get("PUBLISHED")).longValue();
        long pending = ((Number) stats.get("PENDING")).longValue();
        return Map.of(
            "total", seedCount,
            "published", published,
            "pending", pending,
            "ready", pending == 0
        );
    }
}
