/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.kafka.connect;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.master.LocalAlluxioCluster;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests for the {@link AlluxioSinkTask} class.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({SinkTaskContext.class})
public class AlluxioSinkTaskTest {

  private SinkTaskContext mSinkTaskContext;
  private Set<TopicPartition> mAssignments = new HashSet<>();
  private String mTestedTopic;

  /**
   * Sets up the sink task context before a test runs.
   */
  @Before
  public void before() {
    mSinkTaskContext = PowerMockito.mock(SinkTaskContext.class);
    mTestedTopic = "topic_test";
    TopicPartition tp0 = new TopicPartition(mTestedTopic, 0);
    TopicPartition tp1 = new TopicPartition(mTestedTopic, 1);
    mAssignments.add(tp0);
    mAssignments.add(tp1);
    Mockito.when(mSinkTaskContext.assignment()).thenReturn(mAssignments);
  }

  /**
   * Tests that start sink task.
   * @throws Exception when starting sink task
   */
  @Test
  public void startTest() throws Exception {
    Map<String, String> map = new HashMap<>();
    String topicDir = "topics";
    map.put(AlluxioSinkConnectorConfig.TOPICS_DIR, topicDir);
    LocalAlluxioCluster cluster = new LocalAlluxioCluster(Constants.GB, 128 * Constants.MB);
    cluster.start();

    AlluxioSinkTask sinkTask = new AlluxioSinkTask();
    sinkTask.initialize(mSinkTaskContext);
    sinkTask.start(map);

    FileSystem client = cluster.getClient();
    AlluxioURI topicURI = new AlluxioURI("/" + topicDir + "/" + mTestedTopic);
    Assert.assertTrue(client.exists(topicURI));
    List<URIStatus> lstTopicDirStatus = client.listStatus(topicURI);
    Assert.assertEquals(lstTopicDirStatus.size(), 2);

    AlluxioURI topicTmpURI = new AlluxioURI("/" + topicDir + "/tmp/" + mTestedTopic);
    Assert.assertTrue(client.exists(topicTmpURI));
    List<URIStatus> lstTmpTopicDirStatus = client.listStatus(topicTmpURI);
    Assert.assertEquals(lstTmpTopicDirStatus.size(), 2);

    sinkTask.stop();
    cluster.stop();
  }

  /**
   * Tests for putting sink records.
   * @throws Exception when putting sink records
   */
  @Test
  public void putTest() throws Exception {
    Map<String, String> map = new HashMap<>();
    String topicDir = "topics";
    map.put(AlluxioSinkConnectorConfig.TOPICS_DIR, topicDir);
    map.put(AlluxioSinkConnectorConfig.ROTATION_RECORD_NUM, "5");
    LocalAlluxioCluster cluster = new LocalAlluxioCluster(Constants.GB, 128 * Constants.MB);
    cluster.start();

    AlluxioSinkTask sinkTask = new AlluxioSinkTask();
    sinkTask.initialize(mSinkTaskContext);
    sinkTask.start(map);

    final Schema valueSchema = SchemaBuilder.struct().name("record").version(1)
        .field("name", Schema.STRING_SCHEMA)
        .field("id", Schema.INT32_SCHEMA)
        .build();

    Collection<SinkRecord> sinkRecords = new ArrayList<>();
    int recordNum = 10;
    for (int i = 1; i <= recordNum; i++) {
      final Struct record = new Struct(valueSchema)
          .put("name", "Lily")
          .put("id", i);
      SinkRecord sinkRecord = new SinkRecord(mTestedTopic, 0, null, null, valueSchema, record, i);
      sinkRecords.add(sinkRecord);
    }
    sinkTask.put(sinkRecords);

    FileSystem client = cluster.getClient();
    AlluxioURI topicPartitionURI =
        new AlluxioURI("/" + topicDir + "/" + mTestedTopic + "/partition=0");
    Assert.assertTrue(client.exists(topicPartitionURI));
    List<URIStatus> lstTopicPartitionStatus = client.listStatus(topicPartitionURI);
    Assert.assertEquals(lstTopicPartitionStatus.size(), 2);
    for (URIStatus uriStatus : lstTopicPartitionStatus) {
      Assert.assertTrue(uriStatus.getLength() > 0);
    }

    sinkTask.stop();
    cluster.stop();
  }

  /**
   * Tests for partitions closed.
   * @throws Exception when partitions revoked
   */
  @Test
  public void closeTest() throws Exception {
    Map<String, String> map = new HashMap<>();
    String topicDir = "topics";
    map.put(AlluxioSinkConnectorConfig.TOPICS_DIR, topicDir);
    map.put(AlluxioSinkConnectorConfig.ROTATION_RECORD_NUM, "5");
    LocalAlluxioCluster cluster = new LocalAlluxioCluster(Constants.GB, 128 * Constants.MB);
    cluster.start();

    AlluxioSinkTask sinkTask = new AlluxioSinkTask();
    sinkTask.initialize(mSinkTaskContext);
    sinkTask.start(map);

    final Schema valueSchema = SchemaBuilder.struct().name("record").version(1)
        .field("name", Schema.STRING_SCHEMA)
        .field("id", Schema.INT32_SCHEMA)
        .build();

    Collection<SinkRecord> sinkRecords = new ArrayList<>();
    final Struct record = new Struct(valueSchema)
        .put("name", "Lily")
        .put("id", 1);
    SinkRecord sinkRecord = new SinkRecord(mTestedTopic, 0, null, null, valueSchema, record, 0);
    sinkRecords.add(sinkRecord);
    sinkTask.put(sinkRecords);

    Collection<TopicPartition> partitions = new ArrayList<>();
    TopicPartition tp0 = new TopicPartition(mTestedTopic, 0);
    partitions.add(tp0);
    sinkTask.close(partitions);

    FileSystem client = cluster.getClient();
    AlluxioURI topicTmpURI =
        new AlluxioURI("/" + topicDir + "/tmp/" + mTestedTopic + "/partition=0");
    List<URIStatus> lstTmpTopicPartitionStatus = client.listStatus(topicTmpURI);
    Assert.assertEquals(lstTmpTopicPartitionStatus.size(), 0);
  }

  /**
   * Tests for partitions opened.
   * @throws Exception when partitions assigned
   */
  @Test
  public void openTest() throws Exception {
    Map<String, String> map = new HashMap<>();
    String topicDir = "topics";
    map.put(AlluxioSinkConnectorConfig.TOPICS_DIR, topicDir);
    map.put(AlluxioSinkConnectorConfig.ROTATION_RECORD_NUM, "5");
    LocalAlluxioCluster cluster = new LocalAlluxioCluster(Constants.GB, 128 * Constants.MB);
    cluster.start();

    AlluxioSinkTask sinkTask = new AlluxioSinkTask();
    sinkTask.initialize(mSinkTaskContext);
    sinkTask.start(map);
    Collection<TopicPartition> partitions = new ArrayList<>();
    TopicPartition tp0 = new TopicPartition(mTestedTopic, 2);
    partitions.add(tp0);
    sinkTask.open(partitions);

    FileSystem client = cluster.getClient();
    AlluxioURI topicURI = new AlluxioURI("/" + topicDir + "/" + mTestedTopic);
    List<URIStatus> lstTopicDirStatus = client.listStatus(topicURI);
    Assert.assertEquals(lstTopicDirStatus.size(), 3);
  }
}
