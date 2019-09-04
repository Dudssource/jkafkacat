# Jkafkacat

Simple java based library to consume/produce messages from a Kafka broker. This tool was strongly based on the original kafkacat implementation.
The motivation to recreate the (already existing) tool is because the kafkacat needs to have some OS dependencies installed (librdkafka) that in some cases may not be available (my case) and will not be able to have the new features (avro serialization/deserialization) for example.

## Examples

### Consuming all messages from an unsecured broker, from the beginning of the topic

Command:

```bash
java -jar jkafkacat-dist.jar -m consumer -o begin -t mytopic -c config.properties
```

config.properties

```properties
bootstrap.servers=localhost:9092
group.id=jkafkacat
value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
```

### Consuming 10 messages from an unsecured broker and then exit, starting at the end of the topic (will consume only new messages)

Command:

```bash
java -jar jkafkacat-dist.jar -m consumer -o end -nm 10 -t mytopic -c config.properties
```

config.properties

```properties
bootstrap.servers=localhost:9092
group.id=jkafkacat
value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
```

### Given a topic with 5 partitions in an unsecured broker, consume the last message from each partition, consuming an amount of 5 messages and then exit

Command:

```bash
java -jar jkafkacat-dist.jar -m consumer -o end -os -1 -t mytopic -c config.properties -nm 5
```

config.properties

```properties
bootstrap.servers=localhost:9092
group.id=jkafkacat
value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
```

### Given an ordinary topic in an unsecured broker, consume 100 messages produced after 'Thursday, 29 August 2019 13:00:00 GMT', formatting the output to show the message timestamp and the key#value and then exit

Command:

```bash
java -jar jkafkacat-dist.jar -m consumer -o timestamp -ot 1567083600000 -t mytopic -c config.properties -nm 100 -f "%T %k#%v"
```

config.properties

```properties
bootstrap.servers=localhost:9092
group.id=jkafkacat
value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
```

### Given an ordinary topic in an unsecured broker, consume 100 messages produced after 'Thursday, 29 August 2019 13:00:00 GMT', formatting the output to show the message timestamp and the key#value and then exit

Command:

```bash
java -jar jkafkacat-dist.jar -m consumer -o timestamp -ot 1567083600000 -t mytopic -c config.properties -nm 100 -f "%T %k#%v"
```

config.properties

```properties
bootstrap.servers=localhost:9092
group.id=jkafkacat
value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
```

### Given an ordinary topic in a secured broker with SASL_SSL, consume 10 AVRO messages, getting dynamically the schema from a secured schema registry with SSL/Certificate authentication - specified through the ks and ksp params, formatting the output to show the message timestamp and the key#value and then exit

Command:

```bash
java -jar jkafkacat-dist.jar -ks keystore.jks -ksp changeit -m consumer -o end -nm 10 -t mytopic -c config.properties -f "%k#%v"
```

config.properties

```properties
bootstrap.servers=localhost:9092
group.id=jkafkacat
sasl.mechanism=SCRAM-SHA-512
security.protocol=SASL_SSL
ssl.endpoint.identification.algorithm=
ssl.truststore.location=truststore.jks
ssl.truststore.password=changeit
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="admin" \
  password="admin";

# avro deserialization
value.deserializer=io.confluent.kafka.serializers.KafkaAvroDeserializer
key.deserializer=org.apache.kafka.common.serialization.LongDeserializer

# schema registry
schema.registry.url=https://localhost:8081
```

### Given the need to reset the offset of a source kafka-connect connector, extracting the information from the connect-offsets topic to see the partition and key#value

Command:

```bash
java -jar jkafkacat-dist.jar -m consumer -o end -os -1 -t connect-offsets -c config.properties -f "\nKey (%K bytes): %k \nValue (%V bytes): %v \nTimestamp: %T \nPartition: %p \nOffset: %o\n"
```

config.properties

```properties
bootstrap.servers=localhost:9092
group.id=jkafkacat
value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
```

### Given the need to reset the offset of a source kafka-connect connector, in an unsecured broker, produce a tombstone message to a specific partition of the topic connect-offsets

Command:

```bash
echo '["source-file-01",{"filename":"/data/testdata.txt"}]#' | java -jar jkafkacat-dist.jar -m producer -t connect-offsets -c config.properties -K # -p 20
```

config.properties

```properties
bootstrap.servers=localhost:9092
value.serializer=org.apache.kafka.common.serialization.StringSerializer
key.serializer=org.apache.kafka.common.serialization.StringSerializer
```


### Given the need to copy AVRO messages (key and value) from a topic to another, in an unsecured broker, pipe the consume command to the produce command to produce AVRO messages

Command:

```bash
java -jar jkafkacat-dist.jar -m consumer -t source-topic -o begin -c consumer.properties -f "%k#%v" | java -jar jkafkacat-dist.jar -m producer -t target-topic -c producer.properties -K #
```

consumer.properties

```properties
bootstrap.servers=brokerA:9092
value.deserializer=io.confluent.kafka.serializers.KafkaAvroDeserializer
key.deserializer=org.apache.kafka.common.serialization.LongDeserializer
schema.registry.url=http://localhost:8081
```

producer.properties

```properties
bootstrap.servers=brokerB:9092
value.serializer=io.confluent.kafka.serializers.KafkaAvroSerializer
key.serializer=io.confluent.kafka.serializers.KafkaAvroSerializer
schema.registry.url=http://localhost:8081
```