# Skier Consumer Application

This is a consumer application that processes skier lift ride events from a RabbitMQ queue and stores them in a thread-safe data store.

---

## Prerequisites

1. **Java 17+**: Ensure Java 17 or higher is installed.
2. **RabbitMQ**: A RabbitMQ server must be running and accessible.
3. **EC2 Instance**: An AWS EC2 instance to deploy and run the application.

---

## Configuration

### 1. RabbitMQ Configuration

Update the RabbitMQ connection details in the `application.properties` file:

```properties
# RabbitMQ
spring.rabbitmq.host=<RABBITMQ_HOST>  # Replace with your RabbitMQ server host (e.g., 172.31.21.245)
spring.rabbitmq.port=5672             # Default RabbitMQ port
spring.rabbitmq.username=admin        # RabbitMQ username
spring.rabbitmq.password=admin-password # RabbitMQ password
```

---

## Building the Application

Clone the repository:

```bash
git clone <repository-url>
cd <repository-directory>
```

Build the application using Maven:

```bash
mvn clean package
```

This will generate a JAR file in the `target` directory (e.g., `SkierConsumer-1.jar`).

---

## Deploying to EC2

### 1. Copy the JAR to EC2

Use `scp` to copy the JAR file to your EC2 instance:

```bash
scp -i <your-key.pem> target/SkierConsumer-1.jar ec2-user@<EC2_PUBLIC_IP>:/home/ec2-user/application/
```

### 2. Run the Application

SSH into your EC2 instance:

```bash
ssh -i <your-key.pem> ec2-user@<EC2_PUBLIC_IP>
```

Navigate to the application directory:

```bash
cd /home/ec2-user/application
```

Run the application using `nohup` to keep it running in the background:

```bash
nohup java -jar SkierConsumer-1.jar > app.log 2>&1 &
```

### 3. Monitor Logs

To monitor the application logs, use the `tail` command:

```bash
tail -f app.log
```

---

## Application Functionality

- **Consumes Messages**: The application consumes messages from the `liftRideQueue` RabbitMQ queue.
- **Stores Data**: It stores skier lift ride events in a thread-safe data store (`SkierDataStore`).
- **Logs Progress**: The application logs the total number of messages consumed.

---

## Notes

- Ensure the EC2 security group allows inbound traffic on the required ports.

---

# Step-by-Step Process to Create the Schema in DynamoDB

## 1. Create a DynamoDB Table (Primary Table):

- Open the DynamoDB Console.
- Click **Create Table**.
- Enter the Table Name: `LiftRides`
- Define the Primary Key:
  - Partition Key (PK): `skierID` (string)
  - Sort Key (SK): `resortID#seasonID#dayID#timestamp` (string)

- Insert this to create attributes:
```json
{
  "skierID": {
    "S": "skier123#0"
  },
  "resortID#seasonID#dayID#timestamp": {
    "S": "456#2025#day789#1630000000"
  },
  "resortID": {
    "S": "resort456"
  },
  "seasonID": {
    "S": "2025"
  },
  "dayID": {
    "S": "day789"
  },
  "liftID": {
    "S": "25"
  },
  "timestamp": {
    "S": "1630000000"
  },
  "vertical": {
    "S": "250"
  }
}

Primary Table: LiftRides
Attribute	Type	Key Type	Notes
skierID	String	Partition Key	Formatted as skierID#Hash for sharding.
resortID#seasonID#dayID#timestamp	String	Sort Key	Composite key for range queries.
dayID	String	Attribute	Ski day identifier (e.g., "25").
vertical	Number	Attribute	Precomputed as liftID * 10.
liftID	String	Attribute	Lift identifier (e.g., "10").
seasonID	String	Attribute	Season identifier (e.g., "2025").
resortID	String	Attribute	Resort identifier (e.g., "resort456").
timestamp	String	Attribute	Event timestamp (ISO format).

2. Global Secondary Indexes (GSI)
GSI 1: SSD-Index (Skier-Season-Days)
Field	Value	Purpose
PK	skierID#seasonID	Find all days a skier skied in a season.
SK	dayID	Sort by day.
Projection	INCLUDE (vertical, liftID, resortID)	Optimize skier/day queries.
GSI 2: RD-Index (Resort-Day)
Field	Value	Purpose
PK	resortID#dayID	Find all skiers at a resort on a day.
SK	skierID	Unique skier count.
Projection	KEYS_ONLY	Minimal for counting distinct skiers.
GSI 3: CS-Index (Current Season)
Field	Value	Purpose
PK	resortID#seasonID	Avoid hot partitions.
SK	dayID#skierID	Sort by day and skier.
Projection	INCLUDE (liftID, vertical)	Optimize current-season queries.
