# Useful commands

---

1. Create a topic

```
bin/kafka-topics.sh --create --topic <topic-name> --bootstrap-server localhost:9092 --partitions 4 --replication-factor 1
```

2. Create console producer

```
./kafka-console-producer.sh --broker-list localhost:9092 --topic <topic-name>
```

3. Create console consumer

```
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic <topic-name> --from-beginning -property "key.separator= - " --property "print.key=true"
```

4. Create console consumer with group id

```
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-topic --group <group-name>
```

5. List all the topics

```
./kafka-topics.sh --bootstrap-server localhost:9092 --list
```

6. Describe the topics

- All topics

```
./kafka-topics.sh --bootstrap-server localhost:9092 --describe
```

- Specific topic

```
./kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic <topic-name>
```

_Note - All .sh files are in bin directory._
