package com.example.service1_producer.repository;
import com.example.service1_producer.model.User;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class OutboxRepository {
    private final JdbcTemplate jdbcTemplate;

    public void deleteAll() {
        jdbcTemplate.update("DELETE FROM outbox");
        jdbcTemplate.update("DELETE FROM users");
    }
    public void insertUser(User user){
        jdbcTemplate.update(
        "Insert INTO users(id,name,status) VALUES (?,?,?)",
        user.getId(),user.getName(),user.getStatus()
        );
    }
    public void insertOutbox(User user, String payload){
        jdbcTemplate.update(
            "Insert INTO outbox (id,payload,status) VALUES(?,?,'PENDING')",
            user.getId(),payload
        );
    }
    public List<Map<String, Object>> findPendingOutbox() {
        return jdbcTemplate.queryForList(
            "SELECT * FROM outbox WHERE status = 'PENDING' ORDER BY created_at ASC LIMIT 50"
        );
    }
    public void markPublished(String id) {
        jdbcTemplate.update(
            "UPDATE outbox SET status = 'PUBLISHED', published_at = CURRENT_TIMESTAMP WHERE id = ?",
            id
        );
    }
    public void markFailed(String id) {
        jdbcTemplate.update(
            "UPDATE outbox SET status = 'FAILED' WHERE id = ?",
            id
        );
    }
    public Map<String,Object> getStats(){
        Map<String, Object> stats = new java.util.HashMap<>();
        stats.put("PENDING", jdbcTemplate.queryForObject("SELECT COUNT(*) FROM outbox WHERE status = 'PENDING'", Long.class));
        stats.put("PUBLISHED", jdbcTemplate.queryForObject("SELECT COUNT(*) FROM outbox WHERE status = 'PUBLISHED'", Long.class));
        stats.put("FAILED", jdbcTemplate.queryForObject("SELECT COUNT(*) FROM outbox WHERE status = 'FAILED'", Long.class));
        return stats;
    }
    public boolean republish(String id) {
        int updated = jdbcTemplate.update(
            "UPDATE outbox SET status = 'PENDING' WHERE id = ?",
            id
        );
        return updated > 0;
    }

}
