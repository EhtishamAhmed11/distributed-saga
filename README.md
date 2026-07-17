# Resilient Distributed Saga Orchestrator

An evolutionary implementation of the Saga Pattern demonstrating the transition from synchronous, fragile microservices to a highly resilient, event-driven, self-healing architecture using **Apache Kafka**, **PostgreSQL**, **Docker Compose**, and **Node.js**.

---

## Architecture Overview

This project simulates a distributed e-commerce checkout transaction across five decoupled services:

1. **Saga Service**: The orchestrator coordinating the transaction sequence and rollback states.
2. **Order Service**: Manages order creation and final confirmation.
3. **Inventory Service**: Manages catalog stock deduction and reservation.
4. **Payment Service**: Charges cards and processes refunds.
5. **Shipping Service**: Provisions shipments and handles delivery cancelations.

Each service operates with **its own dedicated PostgreSQL database** and communicates exclusively through network boundaries, ensuring strict database isolation.

---

## Architectural Evolution

The system evolved through multiple design iterations to address the standard failure modes of distributed systems:

1. Sync HTTP Microservices
2. Persistent Orchestration State
3. Resilient Compensation & Backoff Retries
4. Idempotency Keys & Stock Gating
5. Asynchronous Kafka Event Streams
6. Transactional Outbox & Idempotent Consumers (Production Grade)

---

### Milestone 1: Synchronous Microservices & Orchestrator

- **Approach**: Services communicated via synchronous HTTP calls. The Saga orchestrator executed standard `POST` and `PATCH` requests down the chain.
- **Problem Introduced**: If the orchestrator crashed midway through a transaction (e.g. after stock was reserved but before payment was charged), all in-memory transaction states were lost. Reserved stock was permanently leaked, leaving the databases in an inconsistent state.

### Milestone 2: Persistent State & Self-Healing Schemas

- **Approach**: A `sagas` table was introduced inside `saga-db`. The orchestrator logged steps (`current_step`, `status`) to the database before calling downstream services. On reboot, the service scanned the database for active/interrupted sagas and resumed from the last known milestone.
- **Problem Introduced**: If a downstream service was temporarily unavailable (e.g. database restart or network blip), HTTP calls failed immediately, aborting the transaction and triggering immediate rollbacks when a simple retry would have succeeded.

### Milestone 3: Compensation Cascades & Exponential Backoff

- **Approach**: Implemented automatic retry scheduler loops. If a dependent service failed, the saga transitioned to `WAITING_FOR_RETRY` and calculated exponential backoff intervals (5s, 10s, 20s, etc.). Background scanners retried the step until successful or until the max retry threshold (5 attempts) was exceeded, triggering a rollback cascade.
- **Problem Introduced**: Because network connections are unreliable, a service might successfully execute an action but fail to return the HTTP response. The orchestrator, assuming failure, retried the request, leading to duplicate stock deductions and double charging on the payment provider.

### Milestone 4: API & Business-Level Idempotency

- **Approach**:
  - **Inventory**: Converted reservation states to `ACTIVE` and `RELEASED`. If a reservation request arrived for an order already marked `ACTIVE`, stock deduction was skipped and success was returned.
  - **Payment**: Implemented an `idempotency_keys` table with row-level locks (`SELECT ... FOR UPDATE`) in `payment-db`. Duplicate requests locked the key, blocked parallel execution, and returned the cached response body.
- **Problem Introduced**: The system was still synchronous. Slow services kept HTTP connection threads open, resulting in cascading latency, thread starvation, and tight temporal coupling.

### Milestone 5: Event-Driven Kafka Messaging

- **Approach**: Migrated the entire microservice network to asynchronous message brokers using **Apache Kafka**. The orchestrator immediately returns `202 Accepted` to the client, while a reactive state machine listens to `saga-events` and publishes commands to `saga-commands`.
- **Problem Introduced**: **The Dual Write Problem**. Updating the local database state and publishing the corresponding Kafka event are two separate operations. If the service crashed _after_ committing the database state but _before_ publishing the Kafka message, the event was lost forever, causing the transaction to hang permanently.

### Milestone 6: Transactional Outbox & Idempotent Consumers (Current)

- **Approach**:
  - **Transactional Outbox**: Direct Kafka broadcasts were removed. Instead, services write the state change and queue the event into a local `outbox` table **within a single atomic database transaction**.
  - **Outbox Worker**: A background scanner inside each service polls the local `outbox` table using `SELECT ... FOR UPDATE SKIP LOCKED` (safeguarding parallel scale-out instances) to publish pending messages.
  - **Idempotent Consumers**: Every message contains a unique `event_id`. Consumers attempt to write this key into a `processed_events` table before running logic. If a duplicate message is delivered by Kafka, the primary key constraint fails (Postgres code `23505`) and the duplicate is ignored.

---

## Detailed Technology Stack

- **Runtime**: Node.js (v22 LTS)
- **Framework**: Express (REST Entrypoint)
- **Databases**: PostgreSQL 15 (dedicated instances)
- **Message Broker**: Apache Kafka / Zookeeper (Confluent v7.3)
- **Client Library**: KafkaJS
- **Containerization**: Docker / Docker Compose

---

## Setup & Execution

### 1. Prerequisite

Ensure Docker and Docker Compose are installed on your local machine.

### 2. Start the Cluster

To build the images, spin up the 5 databases, configure Zookeeper, set up the Kafka topics, and launch the 5 Node.js services, run:

```bash
docker compose up --build
```

---

## Verifying Resiliency Scenarios

### Scenario A: Happy Path Transaction

Submit a checkout request for a Wireless Mouse (`PROD-002`):

```bash
curl -X POST http://localhost:3001/saga/order \
  -H "Content-Type: application/json" \
  -d '{"productId": "PROD-002", "quantity": 1, "totalPrice": 49.99, "address": "123 Main St"}'
```

- **Expected Result**: Immediate HTTP `202 Accepted`. In the console logs, you will see the services coordinate over Kafka in the background, resulting in a successful order confirmation and completed status updates in `saga-db`.

### Scenario B: Self-Healing Outbox Recovery (Crash Test)

To verify the Transactional Outbox pattern's resilience to the Dual Write problem, we simulate a crash during the purchase of a mechanical keyboard (`PROD-003`):

1. **Trigger the Request**:
   ```bash
   curl -X POST http://localhost:3001/saga/order \
     -H "Content-Type: application/json" \
     -d '{"productId": "PROD-003", "quantity": 1, "totalPrice": 129.99, "address": "123 Main St"}'
   ```
2. **Observe the Crash**: The orchestrator receives the stock reservation callback, writes the state update and the `CHARGE_PAYMENT` command to the database, and crashes instantly.
3. **Verify DB State**: Query the orchestrator outbox while the container is down:
   ```bash
   docker exec -it saga-db psql -U postgres -d saga_db -c "SELECT id, event_type, status FROM outbox;"
   ```
   _(You will see the payment event cached in the outbox as `PENDING`)_
4. **Reboot the Orchestrator**:
   ```bash
   docker restart saga-service
   ```
5. **Observe Self-Healing**: On boot, the Outbox Worker automatically picks up the pending row, publishes it to Kafka, and the transaction resumes and completes successfully to `SUCCESS / COMPLETED` without losing messages!
