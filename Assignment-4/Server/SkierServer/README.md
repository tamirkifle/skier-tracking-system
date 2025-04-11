# Skier Server Application

This is a Spring Boot-based server application that processes skier lift ride data and sends it to a RabbitMQ queue. It includes rate limiting, retry logic, and a Dead Letter Queue (DLQ) for fault tolerance.

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

This will generate a JAR file in the `target` directory (e.g., `SkierServer-1.jar`).

---

## Deploying to EC2

### 1. Copy the JAR to EC2

Use `scp` to copy the JAR file to your EC2 instance:

```bash
scp -i <your-key.pem> target/SkierServer-1.jar ec2-user@<EC2_PUBLIC_IP>:/home/ec2-user/application/
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
nohup java -jar SkierServer-1.jar --server.port=8080 > app.log 2>&1 &
```

### 3. Monitor Logs

To monitor the application logs, use the `tail` command:

```bash
tail -f app.log
```

---

## Application Endpoints

- `POST /skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skier/{skierID}`: Submit a lift ride event.
- `GET /skiers/health`: Health check endpoint (returns OK if the server is running).

---

## RabbitMQ Setup

Install RabbitMQ on a separate EC2 instance or use a managed RabbitMQ service.

### Create Queues and Exchanges:

- **Main Queue**: `liftRideQueue`
- **Dead Letter Exchange**: `deadLetterExchange`
- **Dead Letter Queue**: `deadLetterQueue`

You can use the RabbitMQ management console or CLI to create these.

---

## Troubleshooting

### Application Fails to Start:

- Check the `app.log` file for errors.
- Ensure RabbitMQ is running and accessible from the EC2 instance.

### Rate Limiting Issues:

- Adjust the `rate-limiter.initial-capacity` in `application.properties`.
- Monitor the queue depth using the RabbitMQ management console.

### Retry Logic:

- The application retries failed operations up to 1000 times with exponential backoff.

---

## Example Commands

### Submit a Lift Ride Event

```bash
curl -X POST http://<EC2_PUBLIC_IP>:8080/skiers/1/seasons/2025/days/1/skier/123 \
-H "Content-Type: application/json" \
-d '{"liftID": 5, "time": 120}'
```

### Health Check

```bash
curl http://<EC2_PUBLIC_IP>:8080/skiers/health
```

### Stopping the Application

To stop the application, find the process ID and kill it:

```bash
ps aux | grep SkierServerApplication
kill <PID>
```

---

## Notes

- Ensure the EC2 security group allows inbound traffic on port `8080` (or the port you specify).
- Use a load balancer (e.g., AWS ELB) if deploying multiple instances for high availability.

---

