# Distributed Financial Data Platform

A non-blockchain distributed financial data platform with a single Central Authority (CA) core and multiple Processing Agent (PA) regional nodes.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                         CA CORE                              │
│  ┌──────────────────┐      ┌─────────────────────────────┐ │
│  │  Per-Agent       │      │  Consolidated Global        │ │
│  │  Ledgers         │      │  Ledger                     │ │
│  │  (LMDB)          │      │  (LMDB)                     │ │
│  │                  │      │                             │ │
│  │  agent_id +      │      │  ca_offset → entry          │ │
│  │  agent_seq       │      │  (ordered, append-only)     │ │
│  └──────────────────┘      └─────────────────────────────┘ │
│           │                              │                   │
│           │    Transaction Processor     │                   │
│           └──────────┬───────────────────┘                   │
│                      │                                       │
│                      │ gRPC API (port 50051)                │
└──────────────────────┼───────────────────────────────────────┘
                       │
        ┌──────────────┼──────────────┐
        │              │              │
   ┌────▼─────┐   ┌───▼──────┐   ┌──▼───────┐
   │ PA Node 1│   │ PA Node 2│   │ PA Node 3│
   │          │   │          │   │          │
   │ L1 (LMDB)│   │ L1 (LMDB)│   │ L1 (LMDB)│
   │ Pending  │   │ Pending  │   │ Pending  │
   │          │   │          │   │          │
   │ L2 (LMDB)│   │ L2 (LMDB)│   │ L2 (LMDB)│
   │ Final    │   │ Final    │   │ Final    │
   │          │   │          │   │          │
   │Projection│   │Projection│   │Projection│
   └──────────┘   └──────────┘   └──────────┘
```

## Core Components

### CA Core

**Responsibilities:**
- Maintains authoritative ledger of all transactions
- Validates and approves/rejects transaction proposals from PA nodes
- Exports segments and snapshots for PA bootstrap and repair

**Storage:**
- **Per-Agent Ledger (LMDB)**: Tracks each PA node's proposals and decisions
  - Key: `agent_id + agent_seq`
  - Value: `{tx_id, payload, decision, reason, ca_offset}`

- **Consolidated Global Ledger (LMDB)**: Ordered log of all committed transactions
  - Key: `ca_offset` (monotonically increasing)
  - Value: `{tx_id, agent_id, ledger_diffs, metadata, committed_at}`

**APIs:**
- `ProposeTransaction`: PA nodes submit transactions for approval
- `GetLatestOffset`: Query current highest ca_offset
- `GetTransactionRange`: Retrieve range of committed transactions
- `GetTrustedCheckpoint`: Get latest snapshot and segment info
- `GetSegmentData`: Download specific segment
- `GetSnapshotData`: Download specific snapshot

### PA Node

**Responsibilities:**
- Provide low-latency APIs to clients in specific regions
- Maintain two-level log (L1 pending, L2 final)
- Only treat CA-approved transactions as final
- Implement health state machine with repair-first approach

**Storage:**
- **L1 Log (LMDB)**: Pending transactions awaiting CA approval
  - Key: `l1_seq`
  - Value: `{tx_id, payload, status, ca_offset, reason}`
  - Status: `PENDING_CA | COMMITTED_CA | REJECTED_CA`

- **L2 Log (LMDB)**: CA-approved final transactions
  - Key: `ca_offset`
  - Value: `{tx_id, ledger_diffs, applied_at}`
  - Idempotent application

- **Projections (LMDB)**: Read models built from L2
  - Account balances, transaction history, etc.
  - Tracks `valid_up_to_ca_offset`

**APIs:**
- `SubmitTransaction`: Clients submit transactions
- `GetAccountView`: Query account state from projections
- `GetHealth`: Health status and metrics

**Health States:**
- `HEALTHY`: Normal operation, low lag, no gaps
- `DEGRADED`: Elevated CA lag but no data issues
- `REPAIR`: Detected gaps/corruption, actively repairing
- `UNHEALTHY`: Failed repair or exceeded thresholds

## Transaction Flow

### 1. Client Submission
```
Client → PA Node: SubmitTransaction(tx)
PA Node → Client: Receipt(tx_id, l1_seq, ACCEPTED)
PA Node writes to L1 with status=PENDING_CA
```

### 2. CA Proposal
```
PA Node background worker:
  - Poll L1 for PENDING_CA entries
  - Submit to CA: ProposeTransaction(agent_id, agent_seq, tx_id, payload)
  - CA validates and decides: COMMITTED or REJECTED
  - PA updates L1 entry with result
```

### 3. Local Application
```
PA Node background worker:
  - Poll L1 for COMMITTED_CA entries
  - Apply to L2 in ca_offset order (idempotent)
  - Apply ledger diffs to projections
  - Update valid_up_to_ca_offset
```

### 4. Client Query
```
Client → PA Node: GetAccountView(account_id)
PA Node → Client: AccountView(data, valid_up_to_ca_offset)
```

## Building and Running

### Prerequisites
- Java 17+
- Maven 3.9+
- Docker and Docker Compose (for containerized deployment)

### Build

```bash
# Build all modules
mvn clean package

# Build specific module
mvn clean package -pl ca-core -am
mvn clean package -pl pa-node -am
```

### Run Locally

```bash
# Start CA Core
CA_DB_PATH=/tmp/ca-core/db \
CA_STORAGE_PATH=/tmp/ca-core/storage \
java -jar ca-core/target/ca-core-1.0.0-SNAPSHOT.jar

# Start PA Node
AGENT_ID=pa-node-1 \
PA_DB_PATH=/tmp/pa-node-1/db \
CA_HOST=localhost \
CA_PORT=50051 \
java -jar pa-node/target/pa-node-1.0.0-SNAPSHOT.jar
```

### Run with Docker Compose

```bash
# Build and start all services
docker-compose up --build

# Start in background
docker-compose up -d

# View logs
docker-compose logs -f ca-core
docker-compose logs -f pa-node-1

# Stop all services
docker-compose down

# Stop and remove volumes
docker-compose down -v
```

## Testing

### Using grpcurl

```bash
# Check CA Core health
grpcurl -plaintext localhost:50051 grpc.health.v1.Health/Check

# Get latest offset
grpcurl -plaintext localhost:50051 ca.CaCore/GetLatestOffset

# Submit transaction to PA Node
grpcurl -plaintext -d '{
  "account_id": "account-123",
  "operation": "CREDIT",
  "payload": "eyJhbW91bnQiOiAxMDB9"
}' localhost:50053 pa.PaPublic/SubmitTransaction

# Check PA Node health
grpcurl -plaintext localhost:50053 pa.PaPublic/GetHealth
```

### Sample Client (Java)

```java
// Connect to PA Node
ManagedChannel channel = ManagedChannelBuilder
    .forAddress("localhost", 50053)
    .usePlaintext()
    .build();

PaPublicGrpc.PaPublicBlockingStub stub = PaPublicGrpc.newBlockingStub(channel);

// Submit transaction
ClientTx tx = ClientTx.newBuilder()
    .setAccountId("account-123")
    .setOperation("CREDIT")
    .setPayload(ByteString.copyFrom("{\"amount\": 100}".getBytes()))
    .build();

ClientTxReceipt receipt = stub.submitTransaction(tx);
System.out.println("Transaction accepted: " + receipt.getTxId());

// Query account
AccountRequest req = AccountRequest.newBuilder()
    .setAccountId("account-123")
    .build();

AccountView view = stub.getAccountView(req);
System.out.println("Account data: " + view.getAccountData().toStringUtf8());
System.out.println("Valid up to offset: " + view.getValidUpToCaOffset());
```

## Configuration

### CA Core Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CA_DB_PATH` | `/tmp/ca-core/db` | LMDB database path |
| `CA_STORAGE_PATH` | `/tmp/ca-core/storage` | Segment/snapshot storage path |

### PA Node Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `AGENT_ID` | `pa-node-1` | Unique agent identifier |
| `PA_DB_PATH` | `/tmp/pa-node/db` | LMDB database path |
| `CA_HOST` | `localhost` | CA Core hostname |
| `CA_PORT` | `50051` | CA Core port |

## Key Design Decisions

### Why LMDB?

- **Durability**: ACID transactions with fsync on commit
- **Performance**: Memory-mapped I/O, zero-copy reads
- **Simplicity**: Embedded database, no separate server
- **Ordered Keys**: Natural support for range scans by offset
- **Crash Recovery**: Automatic on database open

### Why Two-Level Log (L1/L2)?

- **L1 (Pending)**: Immediate durability for client receipts
- **L2 (Final)**: Only CA-approved transactions, strict ordering
- **Separation**: Clear distinction between pending and final state
- **Idempotency**: L2 prevents double application via tx_id index

### Why Repair-First Health Model?

- **Availability**: Prefer repair over eviction
- **Budgets**: MAX_REPAIR_TIME and MAX_GAP_SIZE prevent infinite repair
- **Observability**: Clear state transitions (HEALTHY → DEGRADED → REPAIR → UNHEALTHY)
- **Automation**: Designed for eventual automation of repair processes

## Offload Pipeline

CA Core periodically exports the consolidated ledger:

**Segments:**
- Chunks of 1000 entries
- Stored as JSON with SHA-256 checksum
- Path: `storage/segments/{start_offset}-{end_offset}.json`

**Snapshots:**
- Generated every 10,000 entries
- Contains full state at specific offset
- Path: `storage/snapshots/{snapshot_offset}.json`

**Bootstrap Flow:**
1. PA Node calls `GetTrustedCheckpoint`
2. Downloads latest snapshot
3. Downloads all segments since snapshot
4. Builds L2 and projections
5. Starts replication from latest offset

## Monitoring and Observability

### Metrics (exposed via Micrometer/Prometheus)

**CA Core:**
- `ca.transaction.proposed.total` - Total proposals received
- `ca.transaction.committed.total` - Total committed
- `ca.transaction.rejected.total` - Total rejected
- `ca.ledger.offset.latest` - Latest ca_offset
- `ca.offload.segments.exported` - Segments exported
- `ca.offload.snapshots.created` - Snapshots created

**PA Node:**
- `pa.transaction.submitted.total` - Client submissions
- `pa.l1.pending.count` - Pending L1 entries
- `pa.l2.offset.latest` - Latest L2 ca_offset
- `pa.health.state` - Current health state (0-3)
- `pa.replication.lag` - CA offset lag
- `pa.projection.valid_up_to` - Projection validity offset

### Logs

All components use SLF4J with Logback:
- Structured logging with context (agent_id, tx_id, ca_offset)
- State transitions logged at WARN level
- Errors logged with full stack traces

## Production Considerations

### Security
- [ ] Enable TLS for gRPC channels
- [ ] Implement authentication/authorization
- [ ] Encrypt data at rest (LMDB encryption)
- [ ] Audit logging for all transactions

### Scalability
- [ ] Horizontal scaling of PA nodes (current design supports this)
- [ ] CA Core sharding by account range (future)
- [ ] Object storage backend (S3/GCS instead of filesystem)

### Operations
- [ ] Automated backup and restore
- [ ] Metrics dashboards (Grafana)
- [ ] Alerting rules (Prometheus Alertmanager)
- [ ] Automated repair triggers
- [ ] Graceful shutdown and restart

### Testing
- [ ] Integration tests
- [ ] Chaos engineering (network partitions, crashes)
- [ ] Load testing
- [ ] Correctness verification (linearizability)

## License

This is a reference implementation for the distributed financial platform spec.

## Contributing

This is a demonstration implementation. For production use, implement:
- Actual business logic validation
- Real ledger diff computation
- Production-grade object storage
- Comprehensive error handling
- Full test coverage
