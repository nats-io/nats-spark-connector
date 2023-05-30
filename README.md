# NATS-SPARK-CONNECTOR

Following are all the current flavors of the nats-spark-connector, each flavor
has its own directory structure. If you are unsure of which flavor to use, just go with 'balanced'.

- **balanced:**
  In this scenario, Nats utilizes a single JetStream partition, using a durable
  and queued configuration to load balance messages across Spark threads, each
  thread contributing to single streaming micro-batch Dataframe at each Spark "pull".
  Current message offset is kept in the Nats queue for the purpose of fault tolerance
  (FT). Spark simply acknowledges each message during a micro-batch 'commit', and
  resends a message if an ack is not received within a pre-set timeframe.

This flavor is contained in the subdirectory **'balanced'**, which
has its own README.md containing further information.

- **partitioned:**
In this scenario, Nats utilizes a number of JetStream partitions named
*<partition_prefix>-0*, *<partition_prefix>-1*, ..., *<partition_prefix>-N*, each
partition containing pre-configured associated subjects. Each partition sends
messages to its own Spark affinity thread, and all partition threads contribute
to a single streaming micro-batch Dataframe at each Spark "pull". Current offset
for each partition is kept in Spark for the purpose of fault tolerance (FT). There
also is an option to always start from each partition's first offset during a Spark
restart, instead of the FT configuration.

This flavor is contained in the subdirectory **'partitioned'**, which has its
own README.md containing further information.

- **filtered:**
This is a future scenario TBD.