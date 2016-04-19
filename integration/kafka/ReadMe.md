##Kafka Alluxio Connect
Kafka Alluxio Connector implements exporting kafka's specific topic data to alluxio filesystem.User can define data format stored in alluxio file,such as JsonFormat.
##Perquisites
- confluent-2.0.1
- alluxio-1.0.1
##Configuration
###Alluxio Connector Configuration 
####Example
Here is the content of `etc/kafka-connect-alluxio/alluxio-connect.properties`
    
	name=alluxio-sink
	connector.class=alluxio.kafka.connect.AlluxioSinkConnector
    tasks.max=1
    topics=test_alluxio
    alluxio.url=alluxio://localhost:19998
    topics.dir=topics
    rotation.num=3
    rotation.time.interval=-1
    alluxio.format=alluxio.kafka.connect.format.JsonFormat

User can configure connector class in this property file,the `topics` specify the topics we want export data from kafka. `alluxio.url` is the alluxio address where we export data to. `topics.dir ` is the top level directory in alluxio filesystem where the kafka data is stored.`rotation.num` specify the kafka record number writen in alluxio file before file closes.`rotation.time.interval` is similar to `rotation.num`, alluxio file will be closed if the time interval between up-to-date write and last rotatime time is beyond `rotation.time.interval`. ` alluxio.format` specify the serialized data fomat stored in alluxio file.
###Connector Worker Configuration
####Example
Here is the sample configuration for a standalone Kafka Connect worker that uses Avro serialization and integrates the the Schema Registry. This sample configuration assumes a local installation of Confluent Platform with all services running on their default ports.
	
	# Bootstrap Kafka servers. If multiple servers are specified, they should be comma-separated.	
	bootstrap.servers=localhost:9092
	key.converter=io.confluent.connect.avro.AvroConverter
	key.converter.schema.registry.url=http://localhost:8081
	value.converter=io.confluent.connect.avro.AvroConverter
	value.converter.schema.registry.url=http://localhost:8081
	# Local storage file for offset data
	offset.storage.file.filename=/tmp/connect.offsets

Confluent Connector and Worker configuration details can be seen [Kafka connector configuration](http://docs.confluent.io/2.0.1/connect/userguide.html#configuring-connectors)
##Setup
- Install and run Alluxio
- Install and run Confulent

    	./bin/zookeeper-server-start etc/kafka/zookeeper.properties &
    	./bin/schema-registry-start etc/schema-registry/schema-registry.properties
    	./bin/kafka-server-start etc/kafka/server.properties
  
	Note:schema-registry-start is opitional.If you need schema registry such as Avro schema registry,this command is essential.

- Run Alluxio connector
  
	`./bin/connect-standalone etc/kafka-connect-alluxio/alluxio-worker-standalone.properties etc/kafka-connect-alluxio/alluxio-connect.properties `
	
- Test with avro console
	
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

	You should see a file with name `/topics/test_alluxio/partition=0/test_alluxio+0+0000000000+0000000002.json`.The file name is encoded as `topic+kafkaPartition+startOffset+endOoffset.format`,which refers to Kafka-Hdfs-Connector.

- Test with standard producer console and Json format   

	If you use Json format as producer input format, the fllowing configuration in worker properties is necessary, and you also make sure that schema-registry service has been started. 
	
	    key.converter=org.apache.kafka.connect.json.JsonConverter
		value.converter=org.apache.kafka.connect.json.JsonConverter
		key.converter.schemas.enable=false
		value.converter.schemas.enable=false

	Start the console to create the topic and write values

    	./bin/kafka-console-producer --topic test_alluxioo \
		--broker-list localhost:9092 
		--value-serializer org.apache.kafka.connect.json.JsonSerializer

	Input data in console
	
    	{"f1": "value1"}
    	{"f1": "value2"}
    	{"f1": "value3"}

	You should see a file in alluxio with name `/topics/test_alluxio/partition=0/test_alluxio+0+0000000000+0000000002.json` as before.
##Distributed Deployment
To start distributed worker,create a worker configuration as standalone mode,and then execute ` connect-distributed ` command.

    ./bin/connect-distributed etc/kafka-connect-alluxio/alluxio-worker-distributed.properties

In distributed mode, You can use the REST API to manage the connectors running in the cluster.You can read [Connectors Configuration](http://http://docs.confluent.io/2.0.1/connect/userguide.html#getting-started "") for more details.Here is an example.

  `curl -X POST -H "Content-Type: application/json" --data '{"name": "alluxio-sink", "config": {"connector.class":"alluxio.kafka.connect.AlluxioSinkConnector", "tasks.max":"1", "topics":"test_alluxio","alluxio.url":"alluxio://localhost:19998","topics.dir":"topics","rotation.num":"3","alluxio.format":"alluxio.kafka.connect.format.JsonFormat" }}' http://localhost:8083/connectors`

 After post a connector to workers, you can send data to specific topic as standalone mode.