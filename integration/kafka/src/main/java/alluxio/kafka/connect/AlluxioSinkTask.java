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
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;
import alluxio.kafka.connect.format.AlluxioFormat;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * This class exports kafka data to Alluxio filesystem.
 */
public final class AlluxioSinkTask extends SinkTask {

  private static final Logger LOG = LoggerFactory.getLogger(AlluxioSinkTask.class);
  private AlluxioSinkConnectorConfig mConfig;
  private final FileSystem mFs = FileSystem.Factory.get();
  private AlluxioFormat mFormat;
  private Map<TopicPartition, AlluxioTopicPartitionWriter> mTpWriters = new HashMap<>();

  /**
   * Parses the alluxio connector configuration.
   *
   * @param props configuration parameters
   */
  @Override
  public void start(Map<String, String> props) {
    mConfig = new AlluxioSinkConnectorConfig(props);
    String formatClassName = mConfig.getString(AlluxioSinkConnectorConfig.ALLUXIO_FORMAT);
    try {
      mFormat = (AlluxioFormat) Class.forName(formatClassName).newInstance();
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      LOG.error("Exception is {}", e.getMessage());
      throw new ConnectException("Failed to initiate class" + formatClassName);
    }

    String alluxioUrl = mConfig.getString(AlluxioSinkConnectorConfig.ALLUXIO_URL);
    String topDir = mConfig.getString(AlluxioSinkConnectorConfig.TOPICS_DIR);
    String topicsUrl = alluxioUrl + "/" + topDir;
    String tmpTopicsUrl = topicsUrl + "/tmp";

    //create topics dir
    AlluxioURI topicsDirPath = new AlluxioURI(topicsUrl);
    try {
      if (!mFs.exists(topicsDirPath)) {
        mFs.createDirectory(topicsDirPath);
      }
      AlluxioURI tmpTopicsDirPath = new AlluxioURI(tmpTopicsUrl);
      if (!mFs.exists(tmpTopicsDirPath)) {
        mFs.createDirectory(tmpTopicsDirPath);
      }
    } catch (IOException | AlluxioException e) {
      LOG.error("Exception is {}", e.getMessage());
      throw new ConnectException("Failed to create " + topDir + " dir");
    }

    Collection<TopicPartition> assignments = context.assignment();
    try {
      initializeTopicPartitionWriter(assignments);
    } catch (IOException | AlluxioException e) {
      LOG.error("Exception is {}", e.getMessage());
      throw new ConnectException("Failed to initialize TopicPartitionWriter");
    }
  }

  /**
   * Initialize {@link AlluxioTopicPartitionWriter} according to task assignments.
   * One TopicPartition corresponds to one stream.
   *
   * @param assignments assigned TopicPartitions for this task
   */
  private void initializeTopicPartitionWriter(Collection<TopicPartition> assignments)
    throws AlluxioException, IOException {
    for (TopicPartition tp : assignments) {
      AlluxioTopicPartitionWriter tpWriter =
          new AlluxioTopicPartitionWriter(mConfig, context, mFormat, mFs, tp);
      tpWriter.initialize();
      mTpWriters.put(tp, tpWriter);
    }
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    for (SinkRecord record : records) {
      String topic = record.topic();
      int partition = record.kafkaPartition();
      TopicPartition topicPartition = new TopicPartition(topic, partition);
      AlluxioTopicPartitionWriter writer = mTpWriters.get(topicPartition);
      if (writer != null) {
        writer.writeToBuffer(record);
      } else {
        LOG.error("Failed to get writer of " + topicPartition.toString());
      }
    }

    for (AlluxioTopicPartitionWriter writer : mTpWriters.values()) {
      writer.writeRecord();
    }
  }

  /**
   * Flushes all records that have been put for the specified topic-partitions. The
   * offsets are provided for convenience, but could also be determined by tracking all offsets
   * included in the SinkRecords passed to put method.
   *
   * @param offsets mapping of TopicPartition to committed offset
   */
  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
  }

  /**
   * The SinkTask uses this method to create writers for newly assigned partitions in case of
   * partition rebalance.
   *
   * @param partitions The list of partitions that are now assigned to the task (may include
   *                   partitions previously assigned to the task)
   */
  @Override
  public void open(Collection<TopicPartition> partitions) {
    for (TopicPartition tp : partitions) {
      AlluxioTopicPartitionWriter writer = mTpWriters.get(tp);
      if (writer == null) {
        AlluxioTopicPartitionWriter tpWriter =
            new AlluxioTopicPartitionWriter(mConfig, context, mFormat, mFs, tp);
        try {
          tpWriter.initialize();
        } catch (IOException | AlluxioException e) {
          LOG.error("Exception is {}", e.getMessage());
          throw new ConnectException("Failed to initialize TopicPartitionWriter");
        }
        mTpWriters.put(tp, tpWriter);
      }
    }
  }

  /**
   * The SinkTask uses this method to close writers for partitions that are no longer
   * assigned to the SinkTask.
   *
   * @param partitions The list of partitions that should be closed
   */
  @Override
  public void close(Collection<TopicPartition> partitions) {
    for (TopicPartition tp : partitions) {
      AlluxioTopicPartitionWriter writer = mTpWriters.get(tp);
      if (writer != null) {
        try {
          writer.close();
        } catch (IOException | AlluxioException e) {
          LOG.error("Failed to close writer for {}", tp.toString());
          LOG.error("Exception is {}", e.getMessage());
        } finally {
          mTpWriters.remove(tp);
        }
      } else {
        LOG.error("Failed to get writer of " + tp.toString());
      }
    }
  }

  /**
   * Performs any cleanup to stop this task.
   */
  @Override
  public void stop() {
  }

  /**
   * Gets the connector version.
   *
   * @return Connector version
   */
  @Override
  public String version() {
    return AlluxioSinkConnectorConfig.ALLUXIO_CONNECTOR_VERSION;
  }
}
