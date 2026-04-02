# Architecture Diagram

## High-Level Component Diagram

```mermaid
flowchart LR
    subgraph Clients[Clients]
        A1[Web App]
        A2[Mobile App]
        A3[Internal Event Producers]
    end

    subgraph API[API Layer]
        B1[API Gateway / Load Balancer]
        B2[Notification API Service]
    end

    subgraph Core[Notification Core]
        C1[Validation + Schema Check]
        C2[Idempotency / Dedup]
        C3[Preferences Resolver]
        C4[Rate Limiter]
        C5[Fan-out Router]
    end

    subgraph Cache[Cache]
        D1[(Redis\nPreferences Cache\nDedup Keys\nRate Counters)]
    end

    subgraph Queue[Queue/Event Bus]
        E1[(Notification Events Topic)]
        E2[(Push Jobs Topic)]
        E3[(Email Jobs Topic)]
        E4[(In-App Jobs Topic)]
        E5[(Retry Topic)]
        E6[(Dead Letter Queue)]
    end

    subgraph Workers[Workers]
        F1[Push Worker Pool]
        F2[Email Worker Pool]
        F3[In-App Worker Pool]
        F4[Retry Worker]
    end

    subgraph Storage[Database Layer]
        G1[(Notification Events DB)]
        G2[(Preferences DB)]
        G3[(In-App Notifications Store)]
        G4[(Delivery Attempts DB)]
    end

    subgraph External[External Providers]
        H1[Push Provider\n(FCM/APNs)]
        H2[Email Provider\n(SES/SendGrid)]
    end

    A1 --> B1
    A2 --> B1
    A3 --> B1
    B1 --> B2

    B2 --> C1 --> C2 --> C3 --> C4 --> C5

    C2 <--> D1
    C3 <--> D1
    C4 <--> D1

    C3 --> G2
    C2 --> G1

    C5 --> E2
    C5 --> E3
    C5 --> E4

    E2 --> F1
    E3 --> F2
    E4 --> F3

    F1 --> H1
    F2 --> H2
    F3 --> G3

    F1 --> G4
    F2 --> G4
    F3 --> G4

    F1 -. failed .-> E5
    F2 -. failed .-> E5
    F3 -. failed .-> E5

    E5 --> F4
    F4 -. max retries .-> E6
    F4 --> E2
    F4 --> E3
    F4 --> E4
```

## Request and Delivery Flow

1. Producer emits an event, or a client calls the create API.
2. Notification service validates payload and checks idempotency.
3. Service loads preferences (cache first, DB fallback) and applies rate limiting.
4. Service publishes channel-specific jobs to queue topics.
5. Workers process jobs asynchronously:
   - Push worker sends to mobile push provider.
   - Email worker sends to email provider.
   - In-app worker stores notification in user inbox store.
6. Delivery attempts are stored for observability and retries.
7. Failed deliveries are retried with backoff; exhausted jobs move to DLQ.

## Why This Scales

- Queue decouples producers from delivery and smooths spikes.
- Stateless API and worker pools scale horizontally.
- Partitioning by user id keeps ordering and distribution balanced.
- Redis offloads hot reads/writes for preferences, dedup, and rate limits.
- Channel isolation prevents one failing provider from taking down all notifications.
