# Confluent Lightning Tables - JDBC Demo


- [Confluent Lightning Tables - JDBC Demo](#confluent-lightning-tables---jdbc-demo)
  - [Disclaimer](#disclaimer)
  - [Prerequisites](#prerequisites)
  - [Configure Your Environment](#configure-your-environment)
  - [Create the topic and register its schema](#create-the-topic-and-register-its-schema)
  - [Produce sample Avro orders](#produce-sample-avro-orders)
  - [Enable the Lightning Table](#enable-the-lightning-table)
  - [Download the JDBC driver and SQLLine](#download-the-jdbc-driver-and-sqlline)
  - [Configure the JDBC connection](#configure-the-jdbc-connection)
  - [Query using JDBC](#query-using-jdbc)
    - [List recent sample orders](#list-recent-sample-orders)
    - [Find shipped orders, most expensive first](#find-shipped-orders-most-expensive-first)
    - [Find orders within a total range](#find-orders-within-a-total-range)
    - [Find all orders for a customer](#find-all-orders-for-a-customer)
    - [Look up a single order](#look-up-a-single-order)
  - [Freshness](#freshness)
  - [Query Lightning Tables with JetBrains DataGrip](#query-lightning-tables-with-jetbrains-datagrip)

Query real-time Kafka data using **Confluent Cloud Lightning Tables**, **JDBC**, and **Visual Studio Code**.

This demo creates an Avro-backed `orders` topic in Confluent Cloud, produces sample orders, enables a Lightning Table for the topic, and runs SQL queries over JDBC.

It is based on [realtime-contextengine-queries](https://github.com/rjmfernandes/realtime-contextengine-queries), but **replaces the Claude Code / MCP query path with JDBC**. No MCP server, AI assistant, or Flink compute pool is required.

Reference: https://docs.confluent.io/cloud/current/lightning-tables/query/jdbc.html

## Disclaimer

The code and/or instructions here available are **NOT** intended for production usage.
It's only meant to serve as an example or reference and does not replace the need to follow actual and official documentation of referenced products.

## Prerequisites

- A Confluent Cloud account with an **AWS Kafka cluster in a region that supports Lightning Tables** (Basic, Standard, Enterprise, or Dedicated).
- Permission to create topics, manage schemas, and enable a Lightning Table.
- Schema Registry enabled in the environment.
- [Confluent CLI](https://docs.confluent.io/confluent-cli/current/install.html), logged in with `confluent login`.
- `kafka-avro-console-producer` on your `PATH` (from Confluent Platform tools).
- [Visual Studio Code](https://code.visualstudio.com/).
- **Java 17 or later** (`java -version`).
- A **Global API key and secret** associated with an identity authorized to query the topic's Lightning Table. Browsing tables also requires topic-listing and metadata permissions.

**Credentials have different purposes:** Kafka and Schema Registry API keys are used to produce Avro records; the Global API key is used for Lightning Table JDBC queries. A Kafka API key is not a substitute for the Global API key.

## Configure Your Environment

Copy the sample file and replace every placeholder with your own Confluent Cloud values:

```sh
cp .env.sample .env
```

For this JDBC version, `.env.sample` should have the following contents:

```dotenv
ENV_ID=env-xxxxxxxx
LKC_ID=lkc-xxxxxxxx
AWS_REGION=us-west-2
TOPIC=orders
BOOTSTRAP_SERVER=pkc-xxxxxxxx.us-west-2.aws.confluent.cloud:9092
KAFKA_API_KEY=your-kafka-api-key
KAFKA_API_SECRET=your-kafka-api-secret
SCHEMA_REGISTRY_URL=https://psrc-xxxxxxxx.us-west-2.aws.confluent.cloud
SCHEMA_REGISTRY_API_KEY=your-schema-registry-api-key
SCHEMA_REGISTRY_API_SECRET=your-schema-registry-api-secret
```

Edit **`.env`** with your actual values:

```bash
set -a
source .env
set +a
confluent login
confluent environment use "$ENV_ID"
confluent kafka cluster use "$LKC_ID"
```

## Create the topic and register its schema

Create the Kafka topic:

```bash
confluent kafka topic create "$TOPIC" --cluster "$LKC_ID"
```

The schema is in [schema.avsc](schema.avsc):

```json
{
  "type": "record",
  "name": "Order",
  "namespace": "demo",
  "fields": [
    { "name": "order_id", "type": "string" },
    { "name": "customer_id", "type": "string" },
    { "name": "status", "type": "string" },
    { "name": "total", "type": "double" }
  ]
}
```

Register it as the topic's value schema:

```bash
confluent schema-registry schema create \
  --subject "$TOPIC-value" \
  --schema schema.avsc \
  --type AVRO
```

## Produce sample Avro orders

Run the Avro console producer:

```bash
kafka-avro-console-producer \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --topic "$TOPIC" \
  --command-property security.protocol=SASL_SSL \
  --command-property sasl.mechanism=PLAIN \
  --command-property sasl.jaas.config="org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$KAFKA_API_KEY\" password=\"$KAFKA_API_SECRET\";" \
  --reader-property schema.registry.url="$SCHEMA_REGISTRY_URL" \
  --reader-property basic.auth.credentials.source=USER_INFO \
  --reader-property basic.auth.user.info="$SCHEMA_REGISTRY_API_KEY:$SCHEMA_REGISTRY_API_SECRET" \
  --reader-property value.schema="$(cat schema.avsc)"
```

Paste each JSON record on a separate line, then press **Ctrl+D**:

```json
{"order_id":"o-1001","customer_id":"c-001","status":"SHIPPED","total":125.50}
{"order_id":"o-1002","customer_id":"c-002","status":"PENDING","total":89.00}
{"order_id":"o-1003","customer_id":"c-001","status":"SHIPPED","total":240.75}
```

## Enable the Lightning Table

In the [Confluent Cloud Console](https://confluent.cloud/):

1. Open **Environments**, select your environment, and open your Kafka cluster.
2. Go to **Topics**.
3. Find `orders` (or the value of `TOPIC`).
4. In the **Lightning Tables** column, select **Enable** and confirm. Alternatively, open the topic's **Settings** tab and choose **Enable Lightning Table**.
5. Wait until the Lightning Table status is **Active**.

The Lightning Table exposes the topic's data for low-latency, read-only SQL queries. The JDBC driver connects to the **Lightning SQL endpoint**.

You can also check readiness using **Confluent CLI 4.82.0+** (CLI login must use your user account, not a Global API key):

```bash
confluent lightning query --sql "SHOW TABLES"
```

Only tables in the **ACTIVE** phase can be queried.

## Download the JDBC driver and SQLLine

Download the versions referenced in Confluent's JDBC documentation:

```bash
mkdir -p drivers

curl -fL \
  -o drivers/confluent-jdbc-1.0.0.jar \
  https://packages.confluent.io/maven/io/confluent/jdbc/confluent-jdbc/1.0.0/confluent-jdbc-1.0.0.jar

curl -fL \
  -o drivers/sqlline-1.12.0-jar-with-dependencies.jar \
  https://repo1.maven.org/maven2/sqlline/sqlline/1.12.0/sqlline-1.12.0-jar-with-dependencies.jar
```

- **JDBC driver class:** `io.confluent.jdbc.avatica.ConfluentJdbcDriver`
- **Lightning JDBC URL prefix:** `jdbc:confluent:lightning://`

Use the **Lightning** prefix. The same Confluent driver also supports Flink, but its `jdbc:confluent:flink://` connection is a different service.

## Configure the JDBC connection

Create the local credentials file and edit its values:

```bash
cp lightning-jdbc.properties.example lightning-jdbc.properties
chmod 600 lightning-jdbc.properties
```

Use a **Global API key** created under **Administration → API keys** in Confluent Cloud. Its identity must have permission to read the topic and query the Lightning Table; cluster-scoped `DeveloperRead` is one suitable role. Keep this key separate from your Kafka and Schema Registry credentials.

The connection URL references the local properties file rather than embedding secrets:

```text
jdbc:confluent:lightning://?propertiesFile=/absolute/path/to/lightning-jdbc.properties
```

## Query using JDBC

Run:

```bash
CP="$PWD/drivers/sqlline-1.12.0-jar-with-dependencies.jar"
CP="$CP:$PWD/drivers/confluent-jdbc-1.0.0.jar"

java -cp "$CP" sqlline.SqlLine \
  -d io.confluent.jdbc.avatica.ConfluentJdbcDriver \
  -u "jdbc:confluent:lightning://?propertiesFile=$PWD/lightning-jdbc.properties" \
  -n "" -p "" \
  --showWarnings=false
```

The empty username/password switches prevent SQLLine from prompting; the JDBC driver reads the Global API credentials from `lightning-jdbc.properties`.

Once connected, discover available tables:

```text
!tables
```

For the sample `orders` topic, run the following queries. Enter **each query inside SQLLine**, terminated with a semicolon.

### List recent sample orders

```sql
SELECT ORDER_ID, CUSTOMER_ID, STATUS, TOTAL
FROM orders
LIMIT 10;
```

### Find shipped orders, most expensive first

```sql
SELECT ORDER_ID, CUSTOMER_ID, STATUS, TOTAL
FROM orders
WHERE STATUS = 'SHIPPED'
ORDER BY TOTAL DESC
LIMIT 10;
```

### Find orders within a total range

```sql
SELECT ORDER_ID, TOTAL
FROM orders
WHERE TOTAL BETWEEN 100 AND 300
ORDER BY TOTAL DESC
LIMIT 10;
```

### Find all orders for a customer

```sql
SELECT ORDER_ID, STATUS, TOTAL
FROM orders
WHERE CUSTOMER_ID = 'c-001'
LIMIT 10;
```

### Look up a single order

```sql
SELECT ORDER_ID, CUSTOMER_ID, STATUS, TOTAL
FROM orders
WHERE ORDER_ID = 'o-1002'
LIMIT 1;
```

Type `!quit` to exit SQLLine.

**Note:** These queries assume your topic is named `orders`. If it has another name, update the `FROM` clause. Topic names containing hyphens or other non-alphanumeric characters must be enclosed in **backticks** (for example, `` FROM `customer-orders` ``). Unquoted SQL identifiers are case-insensitive; quoted identifiers are case-sensitive.

## Freshness

On the the Avro producer terminal publish a new order:

```json
{"order_id":"o-1008","customer_id":"c-003","status":"SHIPPED","total":310.00}
```

In another terminal, run or reopen SQLLine and query the Lightning Table:

```sql
SELECT ORDER_ID, CUSTOMER_ID, STATUS, TOTAL
FROM orders
WHERE ORDER_ID = 'o-1008'
LIMIT 1;
```

The new order should appear after Lightning Table ingestion catches up. This demonstrates querying newly produced Kafka data through a standard JDBC interface.

## Query Lightning Tables with JetBrains DataGrip

JetBrains DataGrip provides a graphical SQL editor with query execution, tabular results, and data export. It is officially supported for Confluent Lightning Tables JDBC connections.

- Install and open DataGrip.
- Open Database Explorer and create a new custom JDBC driver.
- Name it Confluent JDBC.
- Add the downloaded confluent-jdbc-1.0.0.jar under Driver Files.
- Set the driver class: `io.confluent.jdbc.avatica.ConfluentJdbcDriver`
- Create a new data source using this driver.
- Configure the JDBC URL using the same properties file used by SQLLine: `jdbc:confluent:lightning://?propertiesFile=/absolute/path/to/lightning-jdbc.properties`
- Click Test Connection and save.

Execute for example:

```sql
SELECT ORDER_ID, CUSTOMER_ID, STATUS, TOTAL
FROM orders
WHERE ORDER_ID = 'o-1008'
LIMIT 1;
```