# NATS-SPARK-CONNECTORS

This repository contains multiple independent kinds of nats-spark-connectors, each flavor
having its own directory structure, with its own README and SBT build files.

- **V2**
This is currently the newest connector, and the one to now use by default. It starts with version 2.0 to avoid any ambiguity with the 'balanced' connector which only has versions 1.x.

- **balanced:**
  In this scenario, NATS utilizes a single JetStream partition, using a durable
  and queued configuration to load balance messages across Spark threads, each
  thread contributing to single streaming micro-batch Dataframe at each Spark "pull".
  Current message offset is kept in the NATS queue for the purpose of fault tolerance
  (FT). Spark simply acknowledges each message during a micro-batch 'commit', and
  resends a message if an ack is not received within a pre-set timeframe.

- **archived**

- At this point, the partitioned connector is now considered legacy and included only for educational purposes only. It may be removed entirely in the future.

- **partitioned:**
In this scenario, NATS utilizes a number of JetStream partitions named
*<partition_prefix>-0*, *<partition_prefix>-1*, ..., *<partition_prefix>-N*, each
partition containing pre-configured associated subjects. Each partition sends
messages to its own Spark affinity thread, and all partition threads contribute
to a single streaming micro-batch Dataframe at each Spark "pull". Current offset
for each partition is kept in Spark for the purpose of fault tolerance (FT). There
also is an option to always start from each partition's first offset during a Spark
restart, instead of the FT configuration.