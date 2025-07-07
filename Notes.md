# Trade-offs, Assumptions, and Future Improvements

## ✅ Assumptions

### Input Directory Structure
- Input data is expected under data/input/date=YYYY-MM-DD/hour=HH/ with standard file names (customers.json.gz, products.json.gz, etc.).

### Erasure Requests
- Anonymization is triggered based on erasure-requests.json.gz files that contain customer identifiers.
- Erasure requests apply retroactively (e.g., requests from 2020-01-25 apply to data processed on 2020-01-26).

### Stateless Batch Processing
- Each ETL run is stateless and based only on current input and previously persisted reference data.
- No intermediate state or checkpointing is maintained between runs.

### Kafka Topics
- Each domain (e.g., customers, products, transactions) is mapped 1:1 to a Kafka topic.
- Messages are assumed to be newline-delimited JSON (NDJSON) format.

### Data Validation
- Data schema validation is assumed to be sufficient to filter out bad records.
- Invalid records are quarantined with _errors field appended.

## ⚖️ Trade-offs

- Real-time vs. Batch - While the pipeline supports both batch and streaming modes (Kafka), most transformations are designed    around batch logic for simplicity. 

- PushGateway vs. Prometheus HTTP server - Using a PushGateway is easier for short-lived batch jobs but not ideal for long-running services or scraping by Prometheus directly. 

- Schema Validation -  Validation is implemented inline in the processors instead of using full-featured schema libraries (e.g., Pydantic or Marshmallow), prioritizing performance and minimal dependencies. 

- File-based Reference Store - Flat file-based storage (JSON files) is simple to test and manage but doesn't scale well for high-volume datasets or frequent queries — no indexing or efficient lookup. 

- Anonymization Strategy - Anonymization is handled by replacing values with static tokens ("ANONYMIZED") — sufficient for GDPR compliance tests but not suitable for advanced privacy preservation (e.g., hashing, tokenization). 

- Unit Test Mocking | Tests mock filesystem and Prometheus metrics extensively, which speeds up test execution but limits full E2E coverage and integration testing.
 
- Error Handling - Errors are logged and metrics are incremented, but there's no retry logic or dead-letter handling implemented. 

## 🚀 Areas for Future Improvement

### Reference Data Backend
- Replace file-based reference data with a proper key-value store (e.g., Redis, RocksDB) or embedded database (e.g., SQLite) for faster anonymization lookups and updates.

### Advanced Validation
- Integrate schema enforcement using libraries like pydantic, cerberus, or jsonschema for maintainability and better error reporting.

### Streaming Improvements
- Add support for offset commits and retry logic in Kafka consumer.
- Buffer messages and process in micro-batches for efficiency.

### Monitoring Enhancements
- Expose metrics via HTTP for Prometheus scraping instead of using PushGateway.
- Add Grafana alerts and dashboards for common failure patterns and pipeline throughput.

### Test Coverage
- Introduce integration tests using real Kafka and Docker Compose test environments (e.g., via testcontainers or GitHub Actions matrix testing).
- Expand test cases for edge cases (e.g., malformed JSON, duplicate SKUs).

### Error Handling & Quarantine
- Add automatic quarantine inspection dashboards.
- Log errors with better trace context (record ID, source file, etc.).

### Data Lineage
- Add metadata/logging to trace how and when data was transformed, anonymized, or quarantined.

### Dynamic Configuration
- Move configuration from static Python files to .env or YAML configs loaded via pydantic.BaseSettings or dynaconf.

### Security
- Add stricter input validation to guard against injection or malformed payloads in streaming mode.
- Audit logging for sensitive transformations (e.g., anonymization actions).
