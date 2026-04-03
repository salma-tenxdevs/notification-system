# Scalable Notification System

## Overview

This document describes the design of a large-scale notification system capable of delivering **100 million notifications per day** across multiple channels — in-app, push, and email — to **20 million daily active users**.

---

## Architecture Overview

```
                          ┌─────────────────────────────────────────────────┐
                          │                   API Gateway                   │
                          │         (Rate Limiting, Auth, Routing)          │
                          └────────────────────┬────────────────────────────┘
                                               │
                          ┌────────────────────▼────────────────────────────┐
                          │           Notification Service                  │
                          │  - Validates request                            │
                          │  - Checks user preferences                      │
                          │  - Deduplication check (Redis)                  │
                          │  - Publishes to appropriate queues              │
                          └──────┬──────────────┬────────────────┬──────────┘
                                 │              │                │
                   ┌─────────────▼──┐  ┌────────▼───────┐  ┌────▼───────────┐
                   │  In-App Queue  │  │  Push Queue     │  │  Email Queue   │
                   │  (Kafka)       │  │  (Kafka)        │  │  (Kafka)       │
                   └────────┬───────┘  └────────┬────────┘  └────────┬───────┘
                            │                   │                     │
                   ┌────────▼───────┐  ┌────────▼────────┐  ┌────────▼───────┐
                   │  In-App Worker │  │  Push Worker    │  │  Email Worker  │
                   │  (stores to DB)│  │  (APNs/FCM)     │  │  (SendGrid)    │
                   └────────┬───────┘  └─────────────────┘  └────────────────┘
                            │
                   ┌────────▼───────────────────────────────────────────────┐
                   │              PostgreSQL + Redis Cache                  │
                   │    (notification storage, user preferences, dedup)     │
                   └────────────────────────────────────────────────────────┘
```

**Key components:**
- **API Gateway** — entry point for all incoming requests; handles auth and rate limiting
- **Notification Service** — core orchestration layer; validates, deduplicates, fans out to queues
- **Kafka Queues** — one topic per channel (in-app, push, email) for async delivery
- **Workers** — consume from queues and deliver via third-party providers (FCM, APNs, SendGrid)
- **PostgreSQL** — stores in-app notifications and user preferences
- **Redis** — caching for user preferences, deduplication keys, and rate limit counters

---

## API Design

### 1. Create a Notification

**`POST /v1/notifications`**

Triggered by internal services when an event occurs.

**Request:**
```json
{
  "user_id": "usr_123",
  "type": "new_message",
  "channels": ["in_app", "push", "email"],
  "title": "New Message",
  "body": "You received a new message from Alice.",
  "metadata": {
    "sender_id": "usr_456",
    "thread_id": "thread_789"
  }
}
```

**Response `201 Created`:**
```json
{
  "notification_id": "notif_abc123",
  "status": "queued",
  "created_at": "2025-04-01T10:00:00Z"
}
```

---

### 2. Get Notifications for a User

**`GET /v1/users/{user_id}/notifications`**

Used by the client app to fetch in-app notifications.

**Query Parameters:**

| Param    | Type    | Description                             |
|----------|---------|-----------------------------------------|
| `limit`  | integer | Max results (default: 20, max: 100)     |
| `cursor` | string  | Pagination cursor (last seen ID)        |
| `unread` | boolean | Filter to unread only (default: false)  |

**Response `200 OK`:**
```json
{
  "notifications": [
    {
      "notification_id": "notif_abc123",
      "type": "new_message",
      "title": "New Message",
      "body": "You received a new message from Alice.",
      "read": false,
      "created_at": "2025-04-01T10:00:00Z"
    }
  ],
  "next_cursor": "notif_xyz999",
  "unread_count": 5
}
```

---

### 3. Mark Notifications as Read

**`PATCH /v1/users/{user_id}/notifications/read`**

**Request:**
```json
{
  "notification_ids": ["notif_abc123", "notif_def456"]
}
```

**Response `200 OK`:**
```json
{
  "updated": 2
}
```

---

### 4. Update User Notification Preferences

**`PUT /v1/users/{user_id}/preferences`**

**Request:**
```json
{
  "channels": {
    "push": true,
    "email": false,
    "in_app": true
  },
  "quiet_hours": {
    "enabled": true,
    "start": "22:00",
    "end": "08:00",
    "timezone": "America/New_York"
  }
}
```

**Response `200 OK`:**
```json
{
  "updated": true
}
```

---

## Data Model

### `notifications` table (PostgreSQL)

Stores all in-app notifications.

| Column            | Type        | Description                                      |
|-------------------|-------------|--------------------------------------------------|
| `id`              | UUID (PK)   | Unique notification ID                           |
| `user_id`         | UUID (FK)   | Recipient user                                   |
| `type`            | VARCHAR     | Event type (e.g., `new_message`, `order_shipped`)|
| `title`           | VARCHAR     | Short notification title                         |
| `body`            | TEXT        | Full notification body                           |
| `metadata`        | JSONB       | Extra context (sender ID, order ID, etc.)        |
| `read`            | BOOLEAN     | Whether the user has read it                     |
| `created_at`      | TIMESTAMPTZ | When the notification was created                |
| `delivered_at`    | TIMESTAMPTZ | When it was delivered to the client              |

**Indexes:**
- `(user_id, created_at DESC)` — primary query pattern (fetch recent notifications by user)
- `(user_id, read)` — supports unread count queries

---

### `user_preferences` table (PostgreSQL)

| Column         | Type      | Description                                  |
|----------------|-----------|----------------------------------------------|
| `user_id`      | UUID (PK) | The user                                     |
| `push_enabled` | BOOLEAN   | Whether push notifications are on            |
| `email_enabled`| BOOLEAN   | Whether email notifications are on           |
| `in_app_enabled`| BOOLEAN  | Whether in-app notifications are on          |
| `quiet_hours`  | JSONB     | Quiet hours window and timezone              |
| `updated_at`   | TIMESTAMPTZ | Last time preferences were changed         |

**Caching:** User preferences are cached in Redis with a 5-minute TTL to avoid a DB hit on every notification.

---

### `notification_dedup_log` (Redis)

To prevent duplicate notifications, a Redis key is set for each (user_id, event_id) pair with a TTL of 24 hours.

```
Key:   dedup:{user_id}:{event_id}
Value: 1
TTL:   86400 seconds (24 hours)
```

If the key already exists, the notification is dropped before being queued.

---

## Scaling Strategy

### Horizontal Scaling

- **Notification Service**: Stateless — scale horizontally behind a load balancer. Multiple instances can run in parallel.
- **Workers**: Each channel has its own consumer group in Kafka, allowing independent scaling. Push workers can be scaled up separately from email workers.
- **PostgreSQL**: Use read replicas for notification reads (the majority of traffic). Writes go to the primary.

### Sharding

The `notifications` table is sharded by `user_id` using consistent hashing:

- 16 shards initially, each shard on its own PostgreSQL instance.
- `user_id % 16` determines the shard.
- Allows independent scaling of individual shards as usage grows.

### Caching Strategy

| Data                  | Cache Layer | TTL        |
|-----------------------|-------------|------------|
| User preferences      | Redis       | 5 minutes  |
| Unread count          | Redis       | 60 seconds |
| Deduplication keys    | Redis       | 24 hours   |
| Rate limit counters   | Redis       | 1 minute   |

### Kafka Topic Configuration

- **3 topics**: `notifications.in_app`, `notifications.push`, `notifications.email`
- **32 partitions per topic** — allows 32 parallel consumer workers per channel
- **Replication factor: 3** — ensures no message loss on broker failure

---

## Reliability & Failure Handling

### Retries

Workers use an **exponential backoff** retry strategy:

| Attempt | Wait       |
|---------|------------|
| 1st     | immediate  |
| 2nd     | 30 seconds |
| 3rd     | 5 minutes  |
| 4th     | 30 minutes |
| 5th+    | Dead-letter queue (DLQ) |

After 5 failed attempts, the message is sent to a dead-letter queue (DLQ) for manual inspection or alerting. Operations staff can replay DLQ messages after the root cause is resolved.

### Deduplication

Deduplication happens at two levels:

1. **Before queuing** — The Notification Service checks Redis for a dedup key. If found, the notification is dropped.
2. **In the worker** — Workers check an idempotency key before writing to the DB or calling third-party APIs.

This protects against duplicates even in cases of worker crashes or Kafka redelivery.

### Rate Limiting

- Per-user rate limits are enforced in Redis using a sliding window counter.
- Default: max 50 notifications per user per hour.
- If the limit is exceeded, the notification is dropped and logged (not queued).
- This prevents spam in runaway event scenarios.

### Failure Scenarios

| Failure                     | Behavior                                               |
|-----------------------------|--------------------------------------------------------|
| Notification Service crash  | Kafka retains unprocessed messages; workers catch up   |
| Worker crash                | Kafka redelivers message to another worker instance    |
| Redis down                  | Fail open: skip dedup/cache, proceed with DB queries   |
| DB primary down             | Failover to replica within ~30 seconds (auto-failover) |
| FCM/APNs unavailable        | Retry via backoff; fall back to in-app only            |
| SendGrid unavailable        | Retry via backoff; alert on-call if sustained failure  |

---

## Tradeoffs

### 1. Push vs. Pull (In-App Notifications)

**Push model (WebSocket/SSE):** The server pushes new notifications to the client in real time. Low latency and great UX, but maintaining persistent connections for 20M users is resource-intensive.

**Pull model (Polling):** The client polls `GET /notifications` every N seconds. Much simpler to scale, but adds latency and wastes bandwidth.

**Decision:** Use **pull with polling** for now (simpler, cheaper). Add WebSocket push for premium/real-time features later as a targeted optimization.

---

### 2. Consistency vs. Latency

Strong consistency (waiting for all DB replicas to confirm a write before responding) adds significant latency at scale.

**Decision:** Accept **eventual consistency**. Notification writes go to the primary DB and are replicated asynchronously to read replicas. A user may briefly see a stale unread count — this is acceptable for a notification system.

---

### 3. Sync vs. Async Delivery

Delivering notifications synchronously (in the request handler) would be simple but would couple API latency to third-party providers (FCM, email) and create backpressure during spikes.

**Decision:** Use **async delivery via Kafka**. The API responds immediately after queuing the notification. This decouples the API from delivery speed, absorbs traffic spikes naturally, and enables retries without blocking users.

---

## Summary

This design prioritizes simplicity, horizontal scalability, and fault tolerance. By using Kafka as the backbone for async delivery, Redis for fast lookups and deduplication, and PostgreSQL with sharding for persistent storage, the system can comfortably handle 100M notifications/day while remaining operable and debuggable by a small team.
