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
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.kafka.connect.format.AlluxioFormat;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This class writes kafka's specific topic partition data to alluxio filesystem.
 */
public class AlluxioTopicPartitionWriter {

  private static final Logger LOG = LoggerFactory.getLogger(AlluxioSinkTask.class);

  private TopicPartition mTopicPartition;

  private long mOffset;

  private FileOutStream mFileOutStream;

  private FileSystem mFs;

  private long mRecordNum;

  private String mFileTempPath;

  private String mTopicPartitionPath;

  private String mTmpTopicPartitionPath;

  private AlluxioSinkConnectorConfig mConfig;

  private SinkTaskContext mContext;

  private AlluxioFormat mFormat;

  private long mLastRotationTime;

  private long mRotationRecordNum;

  private long mRotationTimeInterval;

  private Queue<SinkRecord> mRecordQueue = new LinkedBlockingQueue<SinkRecord>();

  /**
   * AlluxioTopicPartitionWriter Constructor.
   *
   * @param config  AlluxioSinkConnectorConfig
   * @param context SinkTaskContext
   * @param format  AlluxioFormat
   * @param fs      FileSystem
   * @param tp      TopicPartition
   */
  public AlluxioTopicPartitionWriter(AlluxioSinkConnectorConfig config, SinkTaskContext context,
      AlluxioFormat format, FileSystem fs, TopicPartition tp) {
    mConfig = config;
    mContext = context;
    mFormat = format;
    mTopicPartition = tp;
    mFs = fs;
    mFileOutStream = null;
    mFileTempPath = "";
    mTopicPartitionPath = "";
    mTmpTopicPartitionPath = "";
    mRecordNum = 0;
    mOffset = -1;
    mRotationRecordNum = mConfig.getLong(AlluxioSinkConnectorConfig.ROTATION_RECORD_NUM);
    mRotationTimeInterval = mConfig.getLong(AlluxioSinkConnectorConfig.ROTATION_TIME_INTERVAL);
    mLastRotationTime = System.currentTimeMillis();
  }

  /**
   * Creates topic partition dir and Retrieves kafka offset to continue to consume.
   *
   * @throws IOException if a non-Alluxio exception occurs
   * @throws AlluxioException if an unexpected Alluxio exception is thrown
   */
  public void initialize() throws IOException, AlluxioException {
    String topic = mTopicPartition.topic();
    int partition = mTopicPartition.partition();
    String alluxioURL = mConfig.getString(AlluxioSinkConnectorConfig.ALLUXIO_URL);
    String topicsUrl = alluxioURL + "/" + mConfig.getString(AlluxioSinkConnectorConfig.TOPICS_DIR);

    String topicUrl = topicsUrl + "/" + topic;
    mTopicPartitionPath = topicUrl + "/partition=" + Integer.toString(partition);

    String tmpTopicUrl = topicsUrl + "/tmp/" + topic;
    mTmpTopicPartitionPath = tmpTopicUrl + "/partition=" + Integer.toString(partition);

    //create topic dir
    AlluxioURI topicDirPath = new AlluxioURI(topicUrl);
    if (!mFs.exists(topicDirPath)) {
      mFs.createDirectory(topicDirPath);
    }

    //create topic partition dir
    AlluxioURI topicParitionDirPath = new AlluxioURI(mTopicPartitionPath);
    if (!mFs.exists(topicParitionDirPath)) {
      mFs.createDirectory(topicParitionDirPath);
    }

    //create tmp topic dir
    AlluxioURI tmpTopicDirPath = new AlluxioURI(tmpTopicUrl);
    if (!mFs.exists(tmpTopicDirPath)) {
      mFs.createDirectory(tmpTopicDirPath);
    }

    //create tmp topic partition dir
    AlluxioURI tmpTopicParitionDirPath = new AlluxioURI(mTmpTopicPartitionPath);
    if (!mFs.exists(tmpTopicParitionDirPath)) {
      mFs.createDirectory(tmpTopicParitionDirPath);
    }

    //Remove unClosedFiles from tmpTopicParitionDirPath
    //TODO(GuangHui): Recover by WAL mechanism
    List<URIStatus> lstUnClosedUriStatus = mFs.listStatus(tmpTopicParitionDirPath);
    for (URIStatus unClosedUriStatus : lstUnClosedUriStatus) {
      LOG.info("Delete unComplete file" + unClosedUriStatus.getPath());
      mFs.delete(new AlluxioURI(mConfig.getString("alluxio.url") + unClosedUriStatus.getPath()));
    }

    //Retrieve max offset by parsing file name
    long logOffsetBegin = -1;
    List<URIStatus> lstUriStatus = mFs.listStatus(topicParitionDirPath);
    for (URIStatus closedUriStatus : lstUriStatus) {
      String strFileName = closedUriStatus.getName();
      String strEndOffset = strFileName.split("\\.|\\+")[3];
      long lEndOffset = Long.parseLong(strEndOffset);
      if (lEndOffset > logOffsetBegin) {
        logOffsetBegin = lEndOffset;
      }
    }
    mOffset = logOffsetBegin;
    mContext.offset(mTopicPartition, mOffset + 1);
  }

  /**
   * Write record to buffer.
   *
   * @param record SinkRecord
   */
  public void writeToBuffer(SinkRecord record) {
    mRecordQueue.offer(record);
  }

  /**
   * Write buffer record to alluxio file.
   *
   * @throws FileDoesNotExistException if the given file does not exist
   * @throws IOException               if a non-Alluxio exception occurs
   * @throws AlluxioException          if an unexpected Alluxio exception is thrown
   */
  public void writeRecord() throws FileDoesNotExistException, IOException, AlluxioException {
    long now = System.currentTimeMillis();
    while (!mRecordQueue.isEmpty()) {
      SinkRecord record = mRecordQueue.peek();

      if (mFileOutStream == null) {
        createTmpFile();
      }

      mFormat.writeRecord(mFileOutStream, record);
      mRecordNum++;
      mRecordQueue.poll();

      if (isFileRotated(now)) {
        long beginOffset = mOffset + 1;
        long endOffset = mOffset + mRecordNum;
        String strFormat = "%0" + Integer.toString(AlluxioSinkTaskConstants.OFFSET_LENGTH) + "d";
        String strFileName =
            mTopicPartition.topic() + "+" + mTopicPartition.partition() + "+" + String
                .format(strFormat, beginOffset) + "+" + String.format(strFormat, endOffset)
                + mFormat.getExtension();
        String strNewFilePath = mTopicPartitionPath + "/" + strFileName;
        mFileOutStream.close();
        mFs.rename(new AlluxioURI(mFileTempPath), new AlluxioURI(strNewFilePath));
        mFileOutStream = null;
        mRecordNum = 0;
        mFileTempPath = "";
        mOffset = endOffset;
        mLastRotationTime = System.currentTimeMillis();
      }
    }
  }

  /**
   * Judge file should be rotated, if yes, close the temporary file.
   *
   * @param now
   * @return isRotated
   */
  private boolean isFileRotated(long now) {
    boolean isRotated = false;
    if (mRecordNum >= mRotationRecordNum) {
      isRotated = true;
    } else {
      if (mRotationTimeInterval != -1 && (now - mLastRotationTime) >= mRotationTimeInterval) {
        isRotated = true;
      }
    }

    return isRotated;
  }

  /**
   * Create temporary file for writing kafka record.
   *
   * @throws AlluxioException if an unexpected Alluxio exception is thrown
   * @throws IOException      if a non-Alluxio exception occurs
   */
  private void createTmpFile() throws AlluxioException, IOException {
    UUID id = UUID.randomUUID();
    mFileTempPath = mTmpTopicPartitionPath + "/" + id.toString() + "_tmp" + mFormat.getExtension();
    mFileOutStream = mFs.createFile(new AlluxioURI(mFileTempPath));
  }
}
