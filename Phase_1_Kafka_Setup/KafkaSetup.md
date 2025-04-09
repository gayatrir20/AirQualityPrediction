Installation and Configuration

Apache Kafka installation took some steps, beginning with the installation of Java Development Kit 17 (JDK17) using Homebrew. 
After verification of Java installation using `java -version`, I downloaded the latest Apache Kafka (version 3.6.1) binary package and extracted it in my zsh terminal. 
The intention was to utilize KRaft mode (the newconsensus protocol that has replaced ZooKeeper), but this required the generation of a cluster ID which I did by setting the variable "$KAFKA_CLUSTER_ID = bin/kafka-storage.sh random-".
This generated a UUID that needed to be added to the server configuration. I did experience aproblem with the `meta.properties` file, which caused cluster ID inconsistency.

Resolving Configuration Issues

To fix this issue, I had to:

1. Locate the `meta.properties` file in the Kafka logs directory (specified by `log.dirs` in `server.properties`)
2. Remove the existing `meta.properties` file.
3. Initialize the storage using the newly generated cluster ID by using the command: "bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties"

By following these steps, I was able to run the Kafka server by using "bin/kafka-server-start.sh config/kraft/server.properties."

With Kafka up and running, I created a topic exclusively for air quality data named as air-quality uisng the command: "bin/kafka-topics.sh --create --topic air-quality --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1"
and verified its presence.
