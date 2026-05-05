package com.example.service3_consumer.controller;


import com.example.service3_consumer.model.User;
import com.example.service3_consumer.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequiredArgsConstructor
public class UserController {

    private final UserRepository userRepository;

    @PostMapping("/users")
    public ResponseEntity<Void> receiveUser(@RequestBody User user) {
        int updated = userRepository.update(user);
        if (updated > 0) {
            log.warn("Duplicate record received for id={} — updated in place (Kafka at-least-once delivery)", user.getId());
            return ResponseEntity.ok().build();
        } else {
            userRepository.insert(user);
            log.info("New user saved id={} name={} status={}", user.getId(), user.getName(), user.getStatus());
            return ResponseEntity.status(201).build();
        }
    }
    @DeleteMapping("/users")
    public ResponseEntity<Void> deleteAll() {
        userRepository.deleteAll();
        return ResponseEntity.ok().build();
    }
    @GetMapping("/users")
    public List<User> getAllUsers() {
        return userRepository.findAll();
    }

    @GetMapping("/users/count")
    public Map<String, Long> getCount() {
        return Map.of("count", userRepository.count());
    }

    @GetMapping("/users/{id}")
    public ResponseEntity<User> getUserById(@PathVariable String id) {
        return userRepository.findById(id)
            .map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }
}
