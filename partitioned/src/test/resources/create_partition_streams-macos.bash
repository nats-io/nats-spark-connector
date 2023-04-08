#!/bin/bash

streamtemplate='{ "name": "%s",
  "subjects": [
    "%s"
  ],
  "retention": "limits",
  "max_consumers": -1,
  "max_msgs_per_subject": -1,
  "max_msgs": -1,
  "max_bytes": -1,
  "max_age": 0,
  "max_msg_size": -1,
  "storage": "%s",
  "discard": "old",
  "num_replicas": %d,
  "duplicate_window": 120000000000,
  "sealed": false,
  "deny_delete": false,
  "deny_purge": false,
  "allow_rollup_hdrs": false
}\n'

consumertemplate='{
  "ack_policy": "explicit",
  "ack_wait": 30000000000,
  "deliver_policy": "all",
  "durable_name": "%s",
  "max_ack_pending": %d,
  "max_deliver": -1,
  "max_waiting": 512,
  "replay_policy": "instant",
  "num_replicas": 0
}\n'

echo -n 'Stream and consumer names prefix: '
read NAMEPREFIX
echo -n 'Replicas: '
read REPLICAS
echo -n 'Subject prefix (before the partition number token) [hit return if none]: '
read SUBJPREFIX
echo -n 'Subject suffix (after the partition number token) [hit return if none]: '
read SUBJSUFFIX
echo -n 'Number of partitions: '
read NUMBER_OF_PARTITIONS

STORAGE="memory"
MAX_PENDING="1000"

START=0
END=$NUMBER_OF_PARTITIONS

for (( PARTITION_NUMBER=$START; PARTITION_NUMBER < $END; PARTITION_NUMBER++ )) do
  if [ -n "$SUBJPREFIX" ]
  then
    SUBJECT="$SUBJPREFIX.$PARTITION_NUMBER"
  else
    SUBJECT="$PARTITION_NUMBER"
  fi

  if [ -n "$SUBJSUFFIX" ]
  then
    SUBJECT="$SUBJECT.$SUBJSUFFIX"
  fi

  STREAMNAME="$NAMEPREFIX-$PARTITION_NUMBER"

  printf  "$streamtemplate" $STREAMNAME $SUBJECT $STORAGE $REPLICAS > stream.out
  nats stream add --config stream.out

  CONSUMERNAME="$NAMEPREFIX-durable-$PARTITION_NUMBER"

  printf "$consumertemplate" $CONSUMERNAME $MAX_PENDING > consumer.out
  nats consumer add "$STREAMNAME" "$CONSUMERNAME" --config consumer.out

done
