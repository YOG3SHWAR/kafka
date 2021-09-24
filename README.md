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

4. List all the topics

```
./kafka-topics.sh --bootstrap-server localhost:9092 --list
```

_Note_ - All .sh files are in bin directory.
