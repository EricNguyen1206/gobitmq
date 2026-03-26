# Message Queue (MQ) project using Go

EriOnn-MQ is a lightweight, high-performance Message Broker written in Go, implementing the **AMQP 0-9-1** protocol. It provides a reliable and scalable messaging foundation with a focus on ease of deployment and professional management tools.

## Scope

- AMQP 0-9-1 protocol
- broker
- queue
- producer
- consumer
  - Ack, Nack, Reject
- Direct, Fanout, and Topic exchanges for messaging patterns.
- Durability: Gob snapshot-based persistence for queues and messages to survive restarts.
- HTTP API for Dashboard

## Architecture

```mermaid
graph LR
    P[Producer]
    C[Consumer]
    A[Admin/Dev]

    subgraph Node[Message Queue Server Node]
        AMQP[AMQP Server<br>Port: 5672]
        HTTP[Management API<br>Port: 15672]
        
        subgraph Core[Domain Core Broker]
            EX(Exchange)
            Q(Queue)
        end
        
        Store[Message Store<br>Memory / Durable]
        Disk[(Disk Storage<br>JSON / Gob)]
    end

    P -- BasicPublish --> AMQP
    AMQP -- Route --> EX
    EX -- Binding --> Q
    Q -- Dequeue / consumeLoop --> AMQP
    AMQP -- BasicDeliver --> C

    A -- REST HTTP --> HTTP
    HTTP -. GetSnapshot .-> Core

    Core <--> Store
    Store --> Disk

    classDef ext fill:#eff6ff,stroke:#3b82f6,color:#1e40af,stroke-width:2px;
    classDef net fill:#fef3c7,stroke:#f59e0b,color:#92400e,stroke-width:2px;
    classDef api fill:#fce7f3,stroke:#ec4899,color:#9d174d,stroke-width:2px;
    classDef logic fill:#ffffff,stroke:#4f46e5,color:#3730a3,stroke-width:2px;
    classDef st fill:#d1fae5,stroke:#10b981,color:#065f46,stroke-width:2px;
    classDef dsk fill:#f3f4f6,stroke:#9ca3af,color:#4b5563,stroke-width:2px;

    class P,C,A ext;
    class AMQP net;
    class HTTP api;
    class EX,Q logic;
    class Store st;
    class Disk dsk;
```

## Project Structure

```text
erionn-mq/
├── cmd/                # Entrypoint, initializes the system and connects components.
├── internal/
│   ├── amqp/           # AMQP 0-9-1 protocol handling (Server implementation, frame encoding/decoding, method handling).
│   ├── core/           # Core business logic (Broker coordinator, Exchange routing, Queue management, Bindings).
│   ├── store/          # Data storage layer (Memory-based store, Gob snapshot for durability).
│   ├── config/         # System configuration management (Settings, ENV, Defaults).
│   └── management/     # HTTP Management API & Dashboard (Admin UI, Monitoring endpoints).
└── go.mod              # Module declaration and dependency management.
```

## Getting Started

```bash
# Run the server
go run ./cmd
```

## Future Scope

- Write Ahead log store message
- Graceful shutdown
- Matrix export