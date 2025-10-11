# Requirements

## Project Overview

This project is a distributed arithmetic system that performs basic mathematical operations (addition, subtraction, multiplication, division) using a microservices architecture. Each operation is handled by a dedicated service. The system uses Celery for distributed task execution, RabbitMQ as the message broker, and Redis as the result backend. Chaining and grouping of operations are supported via Celery primitives.

---

## System Architecture

### Core Components

| Component         | Description                                 |
|-------------------|---------------------------------------------|
| Add Service       | Handles addition (+) operations             |
| Subtract Service  | Handles subtraction (-) operations          |
| Multiply Service  | Handles multiplication (*) operations       |
| Divide Service    | Handles division (/) operations             |
| API Entrypoint    | Receives expressions, dispatches tasks      |

### Task Queue Configuration

- **Message Broker:** RabbitMQ
- **Result Backend:** Redis
- **Task Queue System:** Celery

Each worker subscribes to its own queue:

| Worker         | Queue Name   |
|----------------|-------------|
| Add Worker     | add_tasks   |
| Subtract Worker| sub_tasks   |
| Multiply Worker| mul_tasks   |
| Divide Worker  | div_tasks   |

---

## Functional Requirements

### Supported Operations

| Operator | Service          | Description                        |
|----------|------------------|------------------------------------|
| +        | Add Service      | Adds two numbers                   |
| -        | Subtract Service | Subtracts one number from another  |
| *        | Multiply Service | Multiplies two numbers             |
| /        | Divide Service   | Divides one number by another      |

Each service exposes a Celery task (e.g., `tasks.add`, `tasks.subtract`, etc.) for asynchronous invocation and composition.

### Celery Features Used

| Feature      | Purpose                                         |
|--------------|-------------------------------------------------|
| send_task()  | Trigger a task by name                          |
| Signature()  | Encapsulate task details for reuse              |
| chain()      | Sequentially execute tasks                      |
| chord()      | Parallel execution with aggregation callback    |
| get()        | Retrieve result from Celery AsyncResult         |

---

## Use Cases

1. **Simple Addition**
   - Use `app.send_task()` or `app.signature()` for addition.
   - Example: `2 + 3` → Output: `5`

2. **Chaining Mutable Tasks**
   - Use `chain()` with mutable signatures.
   - Example: `((((4 + 8) - 6) * 3) / 2)` → Output: `9.0`

3. **Chaining Immutable Tasks**
   - Use `chain()` with immutable signatures.
   - Example: `4 + 8`, `6 - 3`, `3 * 10`, `14 / 7` → Final Output: `2.0`

4. **Chord (Parallel Tasks + Aggregation)**
   - Use `chord()` for parallel execution and aggregation.
   - Example: `(1 + 1) + (2 + 2) + ... + (9 + 9)` → Output: `90`

5. **Pipe Operator (|) for Chaining**
   - Shorthand for `chain()`.
   - Example: `(((2 * 2) * 8) * 10)` → Output: `320`

6. **Chord with Callback**
   - Use a callback Signature for aggregation.
   - Example: `((1 + 1) + (2 + 2) + ... + (99 + 99))` → Output: `9900`

### xsum Function

- Helper function `xsum()` accumulates the sum of an array of integers.
- Used as a callback in a chord to combine results.

---

## Recommended Stack

| Component        | Technology | Purpose                        |
|------------------|------------|--------------------------------|
| Web Framework    | FastAPI    | API entrypoint                 |
| Task Queue       | Celery     | Distributed task management    |
| Message Broker   | RabbitMQ   | Task message transport         |
| Result Backend   | Redis      | Stores task results            |
| Containerization | Docker     | Service isolation & deployment |
