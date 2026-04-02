# Scalable Notification System Design

## 1. Problem Summary

Design a notification platform that sends user notifications for events such as new messages, shipping updates, and new comments. The platform supports:

- In-app notifications (stored and read later)
- Push notifications (mobile)
- Email notifications

Target scale and assumptions:

- 20 million daily active users
- 100 million notifications per day
- Must handle traffic spikes

Average write traffic is about 1,157 notifications/second:

- $100,000,000 / 86,400 \approx 1,157$

For spikes, we design for at least 10x burst capacity (~12,000 notifications/second).

## 2. High-Level Architecture

See full diagram in [docs/architecture.md](docs/architecture.md).

Core components:

- API Layer: receives notification creation and read requests
- Notification Service: validates, deduplicates, checks preferences, applies rate limits
- Queue/Event Bus: decouples ingestion from delivery (Kafka/RabbitMQ/SQS equivalent)
- Workers: channel-specific delivery workers (in-app, push, email)
- Database: durable storage for notifications and delivery attempts
- Cache: Redis for preferences cache, dedupe keys, and rate-limiting counters

## 3. Functional Requirements Mapping

- Create notification when event happens: `POST /v1/notifications` or internal event ingestion
- Multi-channel delivery: fan-out by channel workers
- Store in-app notifications: persisted in notification store
- Respect user preferences: preference lookup (cache + DB fallback)
- Retry if delivery fails: retry policy with exponential backoff + DLQ
- Avoid duplicates: idempotency key + dedupe cache/store
- Basic rate limiting: token bucket/sliding window per user/channel/type

## 4. API Design

### 4.1 Create Notification

`POST /v1/notifications`

Request:

```json
{
  "eventId": "evt_7f9a2",
  "userId": "user_123",
  "type": "NEW_MESSAGE",
  "channels": ["IN_APP", "PUSH", "EMAIL"],
  "title": "You received a new message",
  "body": "Alex sent you a message",
  "metadata": {
    "conversationId": "conv_456",
    "senderId": "user_888"
  },
  "priority": "HIGH",
  "scheduledAt": "2026-04-03T12:00:00Z",
  "idempotencyKey": "msg_user_123_conv_456_evt_7f9a2"
}
```

Response (accepted async):

```json
{
  "notificationId": "noti_9ab31",
  "status": "QUEUED",
  "createdAt": "2026-04-03T12:00:01Z"
}
```

Possible status codes:

- `202 Accepted`: queued for delivery
- `200 OK`: duplicate request, already processed
- `400 Bad Request`: invalid input
- `429 Too Many Requests`: rate limit exceeded

### 4.2 Get In-App Notifications

`GET /v1/users/{userId}/notifications?cursor=abc123&limit=20&unreadOnly=true`

Response:

```json
{
  "items": [
    {
      "notificationId": "noti_9ab31",
      "type": "NEW_MESSAGE",
      "title": "You received a new message",
      "body": "Alex sent you a message",
      "status": "UNREAD",
      "createdAt": "2026-04-03T12:00:01Z",
      "metadata": {
        "conversationId": "conv_456"
      }
    }
  ],
  "nextCursor": "abc124"
}
```

### 4.3 Mark Notification Read (optional but practical)

`PATCH /v1/users/{userId}/notifications/{notificationId}`

Request:

```json
{
  "status": "READ"
}
```

Response:

```json
{
  "notificationId": "noti_9ab31",
  "status": "READ",
  "updatedAt": "2026-04-03T12:02:10Z"
}
```

## 5. Data Model

Use a polyglot approach:

- OLTP store (PostgreSQL/MySQL) for control tables
- Wide-column or partitioned store (Cassandra/DynamoDB) for high-volume in-app notifications
- Redis for hot cache, dedupe TTL keys, and rate limiting

### 5.1 `user_notification_preferences`

Purpose: per-user, per-type, per-channel settings.

Fields:

- `user_id` (PK part)
- `notification_type` (PK part)
- `channel` (PK part: IN_APP/PUSH/EMAIL)
- `enabled` (bool)
- `updated_at`

Indexes:

- PK `(user_id, notification_type, channel)`

### 5.2 `notification_events`

Purpose: idempotent record of event ingestion.

Fields:

- `idempotency_key` (PK)
- `event_id`
- `user_id`
- `type`
- `payload_json`
- `created_at`

Indexes:

- PK `idempotency_key`
- Secondary index `(user_id, created_at desc)` for debug/audit

### 5.3 `in_app_notifications`

Purpose: user inbox for in-app channel.

Fields:

- `user_id` (partition key)
- `notification_id` (clustering key/time sortable)
- `type`
- `title`
- `body`
- `metadata_json`
- `status` (UNREAD/READ)
- `created_at`
- `expires_at` (optional TTL/retention)

Indexes/access:

- Primary access pattern: by `user_id`, sorted by newest
- Optional GSI/index on `(user_id, status, created_at)` for unread-first queries

### 5.4 `delivery_attempts`

Purpose: track delivery outcomes and retries by channel.

Fields:

- `attempt_id` (PK)
- `notification_id`
- `user_id`
- `channel`
- `provider_message_id`
- `status` (PENDING/SENT/FAILED)
- `error_code`
- `attempt_number`
- `next_retry_at`
- `created_at`

Indexes:

- `(notification_id, channel)`
- `(status, next_retry_at)` for retry scanner

## 6. Scaling Strategy

### 6.1 Horizontal Scaling

- Stateless API servers behind load balancer
- Partitioned queue topics by `user_id` hash to keep ordering per user
- Separate worker pools per channel (push/email/in-app) to isolate failures

### 6.2 Data Partitioning and Sharding

- Shard in-app notification storage by `hash(user_id) % N`
- Keep user data co-located by user shard for predictable reads
- Use time-based bucketing to manage very large partitions

### 6.3 Caching

- Cache preferences in Redis with TTL
- Cache user-level mute/rate-limit state in Redis
- Read-through cache with DB fallback

### 6.4 Backpressure and Spike Handling

- Queue absorbs burst traffic
- Autoscale workers on queue lag and throughput metrics
- Priority queues (critical notifications before low priority)

## 7. Reliability and Failure Handling

### 7.1 Retry Policy

- At-least-once delivery via queue semantics
- Exponential backoff retries (example: 1m, 5m, 15m, 1h)
- Max retry count, then move to DLQ for manual/automated reprocessing

### 7.2 Duplicate Prevention

- Require `idempotencyKey` for create API
- Use Redis `SETNX` + TTL and durable `notification_events` check
- Workers are idempotent: skip if `(notification_id, channel)` already delivered

### 7.3 Channel Provider Failures

- Circuit breaker for external push/email providers
- Fallback provider option (if available)
- Persist failed attempts for observability and replay

### 7.4 High Availability

- Multi-AZ deployment for API, queue brokers, and DB replicas
- Health checks + auto failover
- No single point of failure in synchronous request path

### 7.5 Consistency Model

- Eventual consistency accepted:
  - Notification may appear in-app slightly before/after push/email send
  - User preference updates may take short time to propagate through cache

## 8. Key Tradeoffs

1. Push vs Pull:

- Push gives low latency but needs provider dependencies and retry complexity.
- Pull (fetch in-app) is simpler but users see updates only when opening app.

2. Consistency vs Latency:

- Waiting for all channels synchronously increases latency and failure risk.
- Async fan-out lowers latency and improves availability, but introduces eventual consistency.

3. Sync vs Async Processing:

- Sync delivery gives immediate result but cannot scale well under spikes.
- Async queue-based design handles bursts and isolates failures, with added operational complexity.

4. Fan-out on Write vs Fan-out on Read:

- Fan-out on write speeds reads (good for inbox UX) but increases write/storage cost.
- Fan-out on read reduces write amplification but makes reads slower and more complex.

## 9. End-to-End Request Flow

1. Producer service emits business event (message/order/comment).
2. Notification API receives create request and validates payload.
3. Service checks dedupe key, user preferences, and rate limits.
4. Service writes idempotent event record and publishes channel jobs to queue.
5. Channel workers consume jobs, call providers/store in-app data, and log attempts.
6. Failed jobs are retried with backoff; final failures go to DLQ.
7. Client fetches in-app notifications via paginated API.

## 10. Operational Metrics (What to Monitor)

- API latency (`p50`, `p95`, `p99`)
- Queue lag by topic/partition
- Worker success/failure rate by channel
- Retry and DLQ counts
- Duplicate drop count
- Rate-limit rejection count
- In-app read latency and unread fetch latency

## 11. Security and Privacy (Basic)

- Encrypt PII in transit and at rest
- Signed/authenticated internal APIs
- Audit logs for notification sends and preference changes
- Data retention policy (for old notifications and delivery logs)

## 12. Future Improvements

- User timezone-aware quiet hours
- Digest mode (batch non-urgent notifications)
- ML ranking/prioritization to reduce notification fatigue
- Multi-provider smart routing based on delivery performance
