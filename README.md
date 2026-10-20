# Restaurant Franchise Order Event Processing System

## Overview

This project demonstrates an event-driven order processing system using Java, Apache Kafka, and Docker.

When a restaurant order is created, the producer publishes an order event to Kafka. Three independent consumer services process the same event:

- **Kitchen Service:** Processes orders for preparation.
- **Inventory Service:** Checks stock for ordered items.
- **Customer Notification Service:** Sends order confirmation notifications.

## Technologies Used

- Java 21
- Apache Kafka 4.1.1
- Kafka Java Client
- Docker and Docker Compose
- Maven
- IntelliJ IDEA

## Architecture

    Order Producer
          |
          v
    Kafka Topic: restaurant.orders.created
          |
          +----------------+----------------+
          |                |                |
          v                v                v
    Kitchen Service  Inventory Service  Notification Service
          |                |                |
    kitchen-group   inventory-group   notification-group

Each consumer uses a separate consumer group, allowing all three services to receive and process the same order events independently.

## Order Event Structure

Each order event contains the following fields:

| Field | Description |
|---|---|
| `order_id` | Unique order identifier |
| `restaurant_id` | Identifier of the restaurant |
| `customer_name` | Customer who placed the order |
| `items` | Items included in the order |
| `order_time` | Time when the order was created |
| `order_status` | Current order status |

Example:

```json
{
  "order_id": "ORD-101",
  "restaurant_id": "RESTAURANT-101",
  "customer_name": "Teesha",
  "items": "Pizza, fries",
  "order_time": "2026-10-20T09:58:53Z",
  "order_status": "CREATED"
}
```

## Setup and Execution

### Step 1: Start Kafka

Ensure Docker Desktop is running. From the project directory, execute:

```bash
docker compose up -d
```

Verify that the Kafka container is running:

```bash
docker ps
```

### Step 2: Create and Verify the Kafka Topic

Create the topic with three partitions and a replication factor of one:

```bash
docker exec restaurant-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic restaurant.orders.created \
  --partitions 3 \
  --replication-factor 1
```

If the topic already exists, skip the creation command.

Verify the topic:

```bash
docker exec restaurant-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic restaurant.orders.created
```

The topic has three partitions. The producer uses `restaurant_id` as the message key so that orders for the same restaurant are routed to the same partition, provided the partitioning configuration remains unchanged.

### Step 3: Start the Consumer Services

Open the project in IntelliJ IDEA and run each class separately:

1. `KitchenConsumer.java`
2. `InventoryConsumer.java`
3. `NotificationConsumer.java`

Each service subscribes to `restaurant.orders.created` using its own consumer group.

OR

Open separate terminal windows and start each consumer.

**Kitchen Consumer**

```bash
mvn compile exec:java \
  -Dexec.mainClass="org.example.restaurant.KitchenConsumer"
```

**Inventory Consumer**

```bash
mvn compile exec:java \
  -Dexec.mainClass="org.example.restaurant.InventoryConsumer"
```

**Notification Consumer**

```bash
mvn compile exec:java \
  -Dexec.mainClass="org.example.restaurant.NotificationConsumer"
```

Keep all three consumers running while publishing orders.

### Step 4: Publish Order Events

Run `OrderProducer.java` from IntelliJ IDEA.

Enter the requested order details when prompted. Test with restaurant IDs such as:

- `RESTAURANT-101`
- `RESTAURANT-102`

The producer publishes each order event to Kafka using the restaurant ID as the key.

OR

Run the producer:

```bash
mvn compile exec:java \
  -Dexec.mainClass="org.example.restaurant.OrderProducer"
```

The producer prompts for the following details:

- Order ID
- Restaurant ID (`RESTAURANT-101` or `RESTAURANT-102`)
- Customer Name
- Items

Verify that all three consumer consoles display the published order.

## Testing

### Test 1: Consumer Processing

1. Start all three consumers.
2. Run `OrderProducer.java`.
3. Publish an order.
4. Check the output of all three consumers.

**Expected result:** All three services receive the same order event independently.

### Test 2: Failure Isolation

1. Start all three consumers.
2. Stop `KitchenConsumer.java` in IntelliJ.
3. Publish a new order using `OrderProducer.java`.
4. Verify that Inventory and Notification continue processing the order.
5. Restart `KitchenConsumer.java` and observe its processing.

**Expected result:** Stopping Kitchen Service does not stop the other consumer services. After restarting, Kitchen resumes processing according to its committed offsets.

### Test 3: Event Replay

Kafka allows a consumer group to replay previously published events by resetting its offsets.

#### Step 1: Stop Kitchen Consumer

Stop `KitchenConsumer.java` in IntelliJ. Keep Kafka running.

#### Step 2: Preview the Offset Reset

```bash
docker exec restaurant-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group kitchen-group \
  --topic restaurant.orders.created \
  --reset-offsets \
  --to-earliest \
  --dry-run
```

The consumer group must be inactive before resetting offsets.

#### Step 3: Execute the Offset Reset

```bash
docker exec restaurant-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group kitchen-group \
  --topic restaurant.orders.created \
  --reset-offsets \
  --to-earliest \
  --execute
```

#### Step 4: Restart Kitchen Consumer

Run `KitchenConsumer.java` again in IntelliJ.

**Expected result:** Kitchen Service processes previously published order events again. The Inventory and Notification consumer groups are not reset by this command.

## Project Structure

```text
restaurant-order-kafka/
├── docker-compose.yml
├── pom.xml
└── src/
    └── main/
        └── java/
            └── org/
                └── example/
                    └── restaurant/
                        ├── OrderEvent.java
                        ├── OrderProducer.java
                        ├── KitchenConsumer.java
                        ├── InventoryConsumer.java
                        └── NotificationConsumer.java
```


