#!/bin/bash

KAFKA_HEAP_OPTS="-Xmx512M"

/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server localhost:29092 \
  --alter --add-config 'SCRAM-SHA-512=[iterations=4096,password=password]' \
  --entity-type users --entity-name admin;
