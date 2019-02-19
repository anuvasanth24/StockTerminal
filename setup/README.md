# Setup

### Instructions to setup the pipeline and its different technologies

## Cluster
Pegasus was used to setup my cluster and the technologies

peg up <cluster-name>
Note: yaml file used to setup the master and client nodes of the cluster:

Before installing technologies, setup ssh and aws access:
peg install <cluster-name> ssh
peg install <cluster-name> aws

### Cluster 1:
Install and start Kafka and zookeeper:
peg install kafka-cluster zookeeper
peg service kafka-cluster zookeeper start

peg install kafka-cluster kafka
peg service kafka-cluster kafka start

Create kafka topic:
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper ec2-35-165-113-28.us-west-2.compute.amazonaws.com:2181 --replication-factor 3 --partitions 6 --topic topic-stock

Check topic configuration:

/usr/local/kafka/bin/kafka-topics.sh --describe --zookeeper ec2-35-165-113-28.us-west-2.compute.amazonaws.com:2181 --topic topic-stock


### Cluster 2:
Install and start spark:
peg install spark-cluster spark
peg service spark-cluster spark start

#### Run spark job:
(Note additional jars packages needed under the --packages option)
spark-submit --class StockConsumer --packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.3.0,com.memsql:memsql-connector_2.11:2.0.6 /tmp/anu/StockConsumer-1.0.jar


### Cluster 3: Memsql
Currently using a single node standard edition of Memsql as that is the only one that is free.
Install from Amazon marketplace:
https://aws.amazon.com/marketplace/pp/B011MEZL88?qid=1550562379178&sr=0-3&ref_=srh_res_product_title
