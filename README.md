# Kafka Concurrency Simulator

A hands-on Kafka experiment that demonstrates how **consumer concurrency affects throughput** while keeping the workload, topic, and partition count constant.

The simulator processes the same dataset through the same Kafka pipeline using two consumer groups:

- **12-consumer group:** 12 threads consuming from 12 partitions
- **1-consumer group:** 1 thread consuming from the same 12 partitions

This makes it easy to observe how Kafka partitioning and consumer concurrency influence processing speed.

## Live Demo

**Live:** https://kafka-ui.vercel.app/

## What This Project Demonstrates

Kafka allows messages from different partitions to be processed concurrently.

This project makes that behavior visible by running the same workload through two separate consumer groups with very different concurrency settings.

```text
Same topic
Same 12 partitions
Same transformation logic
Same downstream service
Different consumer concurrency
```

The UI lets you run each consumer group independently and compare how quickly each configuration processes the workload.

## Concurrency Simulator

### 12-Consumer Group

```text
12 threads
12 partitions
```

Each consumer thread can be assigned a partition, allowing the consumer group to process multiple partitions concurrently.

```text
Partition 0  -> Consumer Thread 1
Partition 1  -> Consumer Thread 2
Partition 2  -> Consumer Thread 3
...
Partition 11 -> Consumer Thread 12
```

### 1-Consumer Group

```text
1 thread
12 partitions
```

A single consumer thread is responsible for processing messages from all 12 partitions.

```text
Partition 0  ┐
Partition 1  │
Partition 2  │
...          ├──> Consumer Thread 1
Partition 11 │
             ┘
```

The workload is the same, but the amount of parallel processing is very different.

## What We Built

A Kafka pipeline that:

1. Seeds **30,000 users**
2. Publishes pending records to Kafka
3. Transforms status codes into readable values
4. Sends transformed users to a downstream service
5. Persists the final result
6. Runs the transformation using different consumer concurrency settings

## Architecture

![Kafka Flow](https://kafka-ui.vercel.app/assets/kafkaflow-mMSZ_XV0.png)

```text
┌───────────────────────────────┐
│          Service 1            │
│           Producer            │
│                               │
│ Seeds 30,000 users            │
│ Publishes PENDING rows        │
│ Uses Outbox Pattern           │
└───────────────┬───────────────┘
                │
                ▼
┌───────────────────────────────┐
│            Kafka              │
│                               │
│ Topic: user-raw               │
│ 12 partitions                 │
│ JSON messages                 │
│ Consumer-group offsets        │
└───────────────┬───────────────┘
                │
                ▼
┌───────────────────────────────┐
│          Service 2            │
│         Transformer           │
│                               │
│ 12-thread listener            │
│ 1-thread listener             │
│ JSON -> User                  │
│ Status-code transformation    │
└───────────────┬───────────────┘
                │ HTTP POST
                ▼
┌───────────────────────────────┐
│          Service 3            │
│           Receiver            │
│                               │
│ POST /users                   │
│ Persists transformed users    │
│ H2 database                   │
└───────────────────────────────┘
```

## Service 1 — Producer

Service 1 is responsible for creating and publishing the source workload.

- Seeds **30,000 users** into the database on startup
- Publishes `PENDING` rows to Kafka every 3 seconds
- Marks processed rows as `PUBLISHED` or `FAILED`
- Uses the **Outbox Pattern** so database state and message publishing can be handled reliably

## Kafka — Message Broker

Kafka sits between the producer and transformation service.

- Topic: `user-raw`
- **12 partitions**
- JSON message format
- Topic recreated fresh on Service 1 startup
- Offsets tracked independently for each consumer group

Because the two simulator modes use separate consumer groups, both can process the same topic independently while maintaining their own offsets.

## Service 2 — Transformer

Service 2 contains the core concurrency experiment.

It has two Kafka listeners:

### High-Concurrency Listener

```text
Concurrency: 12
Partitions:  12
```

Designed to allow processing across all 12 partitions concurrently.

### Low-Concurrency Listener

```text
Concurrency: 1
Partitions:  12
```

Processes the same topic using only one consumer thread.

Both listeners perform the same work:

1. Consume JSON from Kafka
2. Deserialize JSON into a `User`
3. Convert the status code into a readable value

```text
1 -> ACTIVE
0 -> INACTIVE
```

4. Send the transformed user to Service 3 over HTTP

This keeps the business logic constant while changing only consumer concurrency.

## Service 3 — Receiver

Service 3 intentionally stays simple so the concurrency experiment remains focused on Kafka consumption.

- Exposes `POST /users`
- Accepts transformed users from Service 2
- Persists the final user into an H2 database
- Contains minimal business logic

## Why 12 Partitions?

Kafka concurrency within a consumer group is bounded by the number of partitions available for assignment.

With 12 partitions:

```text
12 partitions + 12 consumers -> up to 12 consumers can actively process
12 partitions + 1 consumer   -> 1 consumer processes all partitions
12 partitions + 15 consumers -> at most 12 can receive partitions
```

This makes 12 partitions useful for comparing high and low consumer concurrency without changing the topic itself.

## Consumer Groups

The two simulator modes use separate consumer groups.

```text
                       user-raw
                     12 partitions
                          │
            ┌─────────────┴─────────────┐
            │                           │
            ▼                           ▼
   12-Consumer Group            1-Consumer Group
      12 threads                   1 thread
```

Because Kafka stores offsets per consumer group, the two groups can consume the same records independently.

## What I Wanted to Review

I built this project as a practical revision of Kafka concepts I had previously worked with, particularly:

- Topic partitioning
- Consumer groups
- Consumer concurrency
- Partition assignment
- Parallel message processing
- Consumer-group offsets
- Throughput differences caused by concurrency
- Producer/consumer separation
- Kafka-backed service communication
- The Outbox Pattern

Rather than only configuring Kafka and assuming concurrency improves throughput, I wanted a small system where the difference could be **run and observed directly**.

## Tech Stack

**Backend**

- Java
- Spring Boot
- Apache Kafka

**Frontend**

- React
- JavaScript

**Data / Infrastructure**

- H2
- Docker
- Kafka

## Project Structure

```text
Service 1
Producer / Outbox
        │
        ▼
Kafka: user-raw
12 partitions
        │
        ▼
Service 2
Transformer
├── 12-thread consumer group
└── 1-thread consumer group
        │
        ▼
Service 3
Receiver / Persistence
```

## Running the Experiment

From the web UI:

1. Start the **12-Consumer Group**
2. Observe the processing run
3. Run the **1-Consumer Group**
4. Compare the processing behavior and completion time

The important part of the experiment is that the topic, partition count, dataset, and transformation logic remain the same.

Only the **consumer concurrency** changes.

## Key Takeaway

Kafka partitions provide the opportunity for parallelism, but the consumer configuration determines how much of that parallelism is actually used.

```text
Partitions provide parallel lanes.
Consumers determine how many lanes are being processed at once.
```

This project provides a simple visual demonstration of that relationship.

## Related Project

I also built a separate Kafka-based project around distributed transactions:

**Saga Orchestrator**
https://github.com/nandaniluitel/saga-orchestrator

That project focuses on:

- Saga orchestration
- Distributed transactions
- Failure handling
- Compensating transactions

This simulator instead focuses specifically on:

- Kafka partitions
- Consumer groups
- Consumer concurrency
- Throughput
- Offset isolation

## Author

**Nandani Luitel**

GitHub: https://github.com/nandaniluitel
