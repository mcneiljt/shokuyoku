# shokuyoku
This application can run in either `service` or `worker` modes. The `service` consumes JSON bodies from HTTP, converts them into Firehose format and writes them to a Kafka topic. The `worker` consumes the Kafka topic, aggregates events and writes groups of them to an ORC file in S3.

## Usage

The application produces a docker image which determines it's mode based on the first argument.

### Build
`docker build -t shokuyoku:test .`

### Start Service
`docker run shokuyoku:test service`

You should add environment variables for proper execution.

```
KAFKA_TOPIC environment variable should contain the topic to subscribe to. e.g. events
LISTEN_ADDR environment variable should contain the address to listen on. e.g. localhost
LISTEN_PORT environment variable should contain the port to listen on e.g. 8080
```

#### For Local Standalone mode
```
CHECK_SIMILAR=true
ENDIAN=little
FLUSH_MINUTES=5
HIVE_DATABASE=events
HIVE_URL=thrift://localhost:9083
IGNORE_NULLS=true
KAFKA_ERROR_TOPIC=error_firehose
KAFKA_GROUP_ID=filter_2
KAFKA_INPUT_TOPIC=firehose
KAFKA_OUTPUT_TOPIC=firehose_output
KAFKA_POLL_DURATION_MS=5000
KAFKA_SERVERS=localhost:9092
LISTEN_ADDR=0.0.0.0
LISTEN_PORT=3004
ORC_BATCH_SIZE=1000
S3_BUCKET=analytics
S3_PREFIX=test1
SERVICE_KAFKA_TOPIC=firehose
WORKER_KAFKA_TOPIC=firehose_output
DATABASE_URL=jdbc:mysql://localhost:3306/shokuyoku
ERROR_WORKER_KAFKA_TOPIC=error_firehose
DATABASE_USERNAME=admin
DATABASE_PASSWORD=admin

```

### Start Worker
`docker run shokuyoku:test worker`

You should add environment variables for proper execution.

The worker currently requires an additional environment variable (`ORC_SCHEMA`). This environment variable should contain a string-form schema (e.g. `struct<mystring:varchar(255),myint:bigint,myarray:array<varchar(255)>,mybool:boolean,mytimestamp:timestamp>`) for the ORC file to be written. This will be replaced with a Hive query in the future.

```
KAFKA_SERVERS environment variable should contain a comma-separated list of kafka servers. e.g. localhost:9092,localhost:9093
KAFKA_GROUP_ID environment variable should contain the name of the Kafka group. e.g. shokuyoku
KAFKA_TOPIC environment variable should contain the topic to subscribe to. e.g. events
KAFKA_POLL_DURATION_MS environment variable should contain the duration for the Kafka Consumer `poll` method in milliseconds. e.g. 500
FLUSH_MINUTES environment variable should contain the interval between flushes in minutes. e.g. 15
AWS_DEFAULT_REGION environment variable should be set https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html
AWS_ACCESS_KEY_ID environment variable should be set https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html
AWS_SECRET_ACCESS_KEY environment variable should be set https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html
S3_BUCKET environment variable should contain the bucket to write events to. e.g. my-event-bucket
ORC_BATCH_SIZE environment variable should contain the number of records per orc batch. e.g. 1024
KAFKA_SECURITY_PROTOCOL environment variable should be one of PLAINTEXT or SSL
ENABLE_FILTER_ERROR environment variable to enabling sending filtered events to an error topic.a (boolean)
```

## Development

The application is currently written using Java 1.8; the goal is to upgrade this to a newer version after considering the upgrade path for the dependencies. Below is an outline of the architecture as well as some norms found in the code base.

### Architecture

`App.java` is the main class. This simple class interrogates the command arguments for either `service` or `worker`, instantiates the approapriate class and starts it.

`Service.java` is the primary logic for the listening service. It implements a simple web server using Undertow. This web server currently listens on all paths and expects a JSON body. Once JSON is received this service will write format the JSON according to Firehose format (`formats.Firehose`) and send it to the configured Kafka topic (`env.KAFKA_TOPIC`).

`Worker.java` is the top-level logic for consuming Firehose messages from the Kafka topic. During the consumer loop a map of `EventDriver` objects is made for each different `event` name pulled from the topic. Each message received from Kafka is sent to the `addMessage` method of the appropriate `EventDriver` this will add it to the current file. Every `env.FLUSH_MINUTES` the consumer will `flush` the `EventDriver` and commit offsets.

`BasicEventDriver.java` is the only implementation of `EventDriver` currently available. When initialized this `EventDriver` fetchs the schema for the `event` and creates an ORC batch. When `addMessage` is called it flattens the incoming message using `JSONColumnFormat` and then iterates through it adding each value that matches (both name and type) the `schema` for the event. If the message added meets the max `batch.size` the batch is written to a local file. When `flush` is called the local ORC file is closed and pushed to S3.

### Code norms

Environment variables are used for configuration. Environment variables are directly accessed wherever configuration is needed. Both `Service` and `Worker` validate all required environment variables when initialized using the `verifyEnvironment` method.

When a class has a local variable it is directly updated by methods. For example, `setColumns` in the `BasicEventDriver` class directly updates `this.columns` instead of returning an object to be set separately.

Ideally class separation includes interfaces for future expansion. For example `BasicEventDriver` implements the `EventDriver` interface which can later be used to make a separate interchangeable class that uses a different metastore, file format or storage service.

Whenever possible senseless errors/warnings are fixed. For example `main/resources/log4j2.xml` was added to the project with a copy of the default log4j configuration in order to silence log4j complaints.
