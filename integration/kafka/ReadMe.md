##Kafka Alluxio Connector
Kafka Alluxio Connector exports data in kafka to Alluxio filesystem. The connector supports user defined data format and shipped with JSON Format.
##Perquisites
- [confluent-3.0](http://www.confluent.io/download)
- alluxio-1.1.0
##Configuration
###Alluxio Connector Configuration 
####Example
Here is the content of `$CONFLUENT_HOME/etc/kafka-connect-alluxio/alluxio-connect.properties`
    
	name=alluxio-sink
	connector.class=alluxio.kafka.connect.AlluxioSinkConnector
	tasks.max=1
	topics=test_alluxio
	alluxio.url=alluxio://localhost:19998
	topics.dir=topics
	rotation.num=3
	rotation.time.interval.ms=-1
	alluxio.format=alluxio.kafka.connect.format.JsonFormat
	retry.time.interval.ms=5000

User can configure connector class in this property file, the `topics` specify the topics we want export data from kafka. `alluxio.url` is the alluxio address where we export data to. `topics.dir ` is the top level directory in alluxio filesystem where the kafka data is stored. `rotation.num` specify the kafka record number writen in alluxio file before file closes. `rotation.time.interval.ms` is similar to `rotation.num`, alluxio file will be closed if the time interval between up-to-date write and last rotatime time is beyond `rotation.time.interval.ms`. `alluxio.format` specify the serialized data fomat stored in alluxio file. `retry.time.interval.ms` specify the retry time interval after failure.
###Connector Worker Configuration
####Example
Here is the sample configuration for a standalone Kafka Connect worker that uses JSON converter. This sample configuration assumes a local installation of Confluent Platform with all services running on their default ports for running Kafka Connect in standalone mode. [Confluent Platform Quick Start Guide ](http://docs.confluent.io/3.0.0/quickstart.html).
	
	# Bootstrap Kafka servers. If multiple servers are specified, they should be comma-separated.	
	bootstrap.servers=localhost:9092
	key.converter=org.apache.kafka.connect.json.JsonConverter
	value.converter=org.apache.kafka.connect.json.JsonConverter
	key.converter.schemas.enable=false
	value.converter.schemas.enable=false
	# Local storage file for offset data
	offset.storage.file.filename=/tmp/connect.offsets

Confluent Connector and Worker configuration details can be seen [Kafka connector configuration](http://docs.confluent.io/3.0.0/connect/userguide.html#configuring-connectors)
##Setup
- Install and run [Alluxio](http://alluxio.org/documentation/master/en/Getting-Started.html)
- Install and run [Confulent](http://docs.confluent.io/3.0.0/)
    
    	.$CONFLUENT_HOME/bin/zookeeper-server-start etc/kafka/zookeeper.properties
    	.$CONFLUENT_HOME/bin/kafka-server-start etc/kafka/server.properties
    	.$CONFLUENT_HOME/bin/schema-registry-start etc/schema-registry/schema-registry.properties
    	
	Note:schema-registry-start is opitional. If you need schema registry such as Avro schema registry, this command is essential.
- Configure Alluxio connector run environment
  
	Create `kafka-connect-alluxio` dir in confluent `share/java` dir, put `alluxio-integration-kafka-connect` relevant jars into newly-created `kafka-connect-alluxio` dir, then create `kafka-connect-alluxio` dir in confluent `etc` dir, put alluxio connector property files(such as `alluxio-connect.properties` etc.) into the dir.

- Run Alluxio connector
    
    	export CLASSPATH=$CONFLUENT_HOME/share/java/kafka-connect-alluxio/*
    
    	$CONFLUENT_HOME/bin/connect-standalone etc/kafka-connect-alluxio/alluxio-worker-standalone.properties etc/kafka-connect-alluxio/alluxio-connect.properties
	
- Test with Standard Console Producer and JSON Format   

	If you use JSON format as producer input format, the fllowing configuration in worker properties is necessary.
	
	    key.converter=org.apache.kafka.connect.json.JsonConverter
		value.converter=org.apache.kafka.connect.json.JsonConverter
		key.converter.schemas.enable=false
		value.converter.schemas.enable=false

	Start the console to create the topic and write values

    	$CONFLUENT_HOME/bin/kafka-console-producer --topic test_alluxio \
		--broker-list localhost:9092 
		--value-serializer org.apache.kafka.connect.json.JsonSerializer

	Input data in console
	
    	{"f1": "value1"}
    	{"f1": "value2"}
    	{"f1": "value3"}

	You should see a file in alluxio with name `/topics/test_alluxio/partition=0/test_alluxio+0+0000000000+0000000002.json`. The file name is encoded as `topic+kafkaPartition+startOffset+endOoffset.format`.

- Test with Avro Console Producer
	
	If you use Avro format as producer input format, the fllowing configuration in worker properties is necessary, and you also make sure that schema-registry service has been started. 
	
    	key.converter=io.confluent.connect.avro.AvroConverter
    	key.converter.schema.registry.url=http://localhost:8081
    	value.converter=io.confluent.connect.avro.AvroConverter
    	value.converter.schema.registry.url=http://localhost:8081

	Start the console to create the topic and write values

    	./bin/kafka-avro-console-producer \
        --broker-list localhost:9092 --topic test_alluxio \
        --property value.schema='{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"}]}'

	Input data in console
	
    	{"f1": "value1"}
    	{"f1": "value2"}
    	{"f1": "value3"}

	You should see a file with name `/topics/test_alluxio/partition=0/test_alluxio+0+0000000000+0000000002.json`. 
##Distributed Deployment
To run Kafka Connect in distributed mode, modify the worker configuration, and then execute ` connect-distributed ` command.

    ./bin/connect-distributed etc/kafka-connect-alluxio/alluxio-worker-distributed.properties

In distributed mode, You can use the [REST API](http://docs.confluent.io/3.0.0/connect/userguide.html#rest-interface) to manage the connectors running in the cluster. You can read [Connectors Configuration](http://docs.confluent.io/3.0.0/connect/userguide.html#configuring-connectors) for more details. Here is an example.

	curl -X POST -H "Content-Type: application/json" --data '{"name": "alluxio-sink", "config": {"connector.class":"alluxio.kafka.connect.AlluxioSinkConnector", "tasks.max":"1", "topics":"test_alluxio","alluxio.url":"alluxio://localhost:19998","topics.dir":"topics","rotation.num":"3","alluxio.format":"alluxio.kafka.connect.format.JsonFormat" }}' http://localhost:8083/connectors

After the connector is created, the data in Kafka for the specified topics will be continuously export to Alluxio.
