package com.example.service3_consumer.repository;

import com.example.service3_consumer.model.User;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class UserRepository {
    private final JdbcTemplate jdbcTemplate;
    public int update(User user) {
        return jdbcTemplate.update(
            "UPDATE users SET name = ?, status = ? WHERE id = ?",
            user.getName(), user.getStatus(), user.getId()
        );
    }
    public void deleteAll() {
        jdbcTemplate.update("DELETE FROM users");
    }
    public void insert(User user) {
        jdbcTemplate.update(
            "INSERT INTO users (id, name, status) VALUES (?, ?, ?)",
            user.getId(), user.getName(), user.getStatus()
        );
    }

    public List<User> findAll() {
        return jdbcTemplate.query(
            "SELECT * FROM users ORDER BY created_at DESC",
            (rs, rowNum) -> new User(
                rs.getString("id"),
                rs.getString("name"),
                rs.getString("status")
            )
        );
    }

    public long count() {
        Long result = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM users", Long.class);
        return result == null ? 0 : result;
    }

    public Optional<User> findById(String id) {
        List<User> results = jdbcTemplate.query(
            "SELECT * FROM users WHERE id = ?",
            (rs, rowNum) -> new User(
                rs.getString("id"),
                rs.getString("name"),
                rs.getString("status")
            ),
            id
        );
        return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
    }
}


