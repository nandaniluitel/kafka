package com.example.service2_transformer.repository;

import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class StatusMappingRepository {
    private final JdbcTemplate jdbcTemplate;

    public String getMappedStatus(String rawStatus) {
        List<String> results = jdbcTemplate.queryForList(
            "SELECT mapped_status FROM status_mapping WHERE raw_status = ?",
            String.class,
            rawStatus
        );
        return results.isEmpty() ? "UNKNOWN" : results.get(0);
    }
}
