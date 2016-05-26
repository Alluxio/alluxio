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
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;

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
  private long mFailureTime;
  private long mTimeout;
  private String mFileTempPath;
  private String mTopicPartitionPath;
  private String mTmpTopicPartitionPath;
  private AlluxioSinkConnectorConfig mConfig;
  private SinkTaskContext mContext;
  private AlluxioFormat mFormat;
  private long mLastRotationTime;
  private long mRotationRecordNum;
  private long mRotationTimeInterval;
  private AlluxioTopicPartitionWriter.State mState;
  private Queue<SinkRecord> mRecordQueue = new LinkedList<>();

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
    mFailureTime = -1;
    mTimeout = mConfig.getLong(AlluxioSinkConnectorConfig.RETRY_TIME_INTERVAL_MS);
    mRotationRecordNum = mConfig.getLong(AlluxioSinkConnectorConfig.ROTATION_RECORD_NUM);
    mRotationTimeInterval = mConfig.getLong(AlluxioSinkConnectorConfig.ROTATION_TIME_INTERVAL_MS);
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
    List<URIStatus> lstUnClosedUriStatus = mFs.listStatus(tmpTopicParitionDirPath);
    for (URIStatus unClosedUriStatus : lstUnClosedUriStatus) {
      LOG.info("Delete unComplete file" + unClosedUriStatus.getPath());
      mFs.delete(new AlluxioURI(mConfig.getString("alluxio.url") + unClosedUriStatus.getPath()));
    }
    //Retrieve max offset by parsing file name
    long logOffsetBegin = retrieveOffset(topicParitionDirPath);
    if (logOffsetBegin != -1) {
      mOffset = logOffsetBegin + 1;
      mContext.offset(mTopicPartition, mOffset);
    }
    mState = State.WRITE_STARTED;
  }

  /**
   * Retrieves current offset from alluxio filename.
   *
   * @param topicParitionDirPath  TopicPartition Uri Path
   * @return current offset of topic partition
   * @throws AlluxioException if an unexpected Alluxio exception is thrown
   * @throws IOException  if a non-Alluxio exception occurs
   */
  private long retrieveOffset(AlluxioURI topicParitionDirPath)
    throws AlluxioException, IOException {
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
    return logOffsetBegin;
  }

  /**
   * Writes record to buffer.
   *
   * @param record SinkRecord
   */
  public void writeToBuffer(SinkRecord record) {
    mRecordQueue.offer(record);
  }

  /**
   * Writes buffer record to alluxio file.
   *
   * @throws FileDoesNotExistException if the given file does not exist
   * @throws IOException               if a non-Alluxio exception occurs
   * @throws AlluxioException          if an unexpected Alluxio exception is thrown
   */
  public void writeRecord() {
    long now = System.currentTimeMillis();
    boolean bResetOffset = false;
    while (!mRecordQueue.isEmpty()) {
      try {
        switch (mState.ordinal()) {
          case 0:
            mContext.pause(new TopicPartition[] {mTopicPartition});
            mState = State.WRITE_PARTITION_PAUSED;
            /*fall through*/
          case 1:
            SinkRecord record = mRecordQueue.peek();
            writeToAlluxio(record);
            mRecordQueue.poll();
            if (!isFileRotated(now)) {
              break;
            }
            mState = State.SHOULD_ROTATE;
            /*fall through*/
          case 2:
            closeTmpFile();
            mState = State.TEMPFILE_CLOSED;
            /*fall through*/
          case 3:
            commitTmpFile();
            mState = State.FILE_COMMITTED;
            /*fall through*/
          case 4:
            mState = State.WRITE_PARTITION_PAUSED;
            break;
          default:
            LOG.error("{} is not a valid state when writing record to topic partition {}.",
                mState, this.getTopicPartition());
        }
      } catch (AlluxioException | IOException e) {
        LOG.error("Exception is {}", e.getMessage());
        if (mFailureTime != -1) {
          mFailureTime = System.currentTimeMillis();
          mRecordQueue.clear();
          mRecordNum = 0;
          mFileOutStream = null;
          mContext.offset(mTopicPartition, mOffset);
          bResetOffset = true;
          mContext.resume(new TopicPartition[] {mTopicPartition});
          mState = State.WRITE_STARTED;
          break;
        } else {
          mFailureTime = System.currentTimeMillis();
          mContext.timeout(mTimeout);
          throw new RetriableException("Attempt to retry...");
        }
      }
    }
    if (!bResetOffset) {
      mFailureTime = -1;
      if (mRecordQueue.isEmpty()) {
        mContext.resume(new TopicPartition[] {mTopicPartition});
        mState = State.WRITE_STARTED;
      }
    }
  }

  /**
   * Writes record to Alluxio file.
   * @param record SinkRecord form Kafka connect
   * @throws IOException   if a non-Alluxio exception occurs
   * @throws AlluxioException if an unexpected Alluxio exception is thrown
   */
  private void writeToAlluxio(SinkRecord record) throws IOException, AlluxioException {
    if (mFileOutStream == null) {
      createTmpFile();
    }
    long recordOffset = record.kafkaOffset();
    boolean bOutOfOrder = false;
    if (mOffset == -1) {
      mOffset = recordOffset;
    } else {
      long nextOffset = mOffset + mRecordNum;
      if (recordOffset != nextOffset) {
        LOG.info("Ingoring the received record offset {},which is not expected offset {}!!",
            Long.toString(recordOffset), Long.toString(nextOffset));
        bOutOfOrder = true;
      }
    }

    if (!bOutOfOrder) {
      mFormat.writeRecord(mFileOutStream, record);
      mRecordNum++;
    }
  }

  /**
   * Closes temporary file on Alluxio.
   * @throws IOException  if a non-Alluxio exception occurs
   */
  private void closeTmpFile() throws IOException {
    if (mFileOutStream != null) {
      mFileOutStream.close();
    }
  }

  /**
   * Deletes temporary file which is not committed.
   * @throws AlluxioException if an unexpected Alluxio exception is thrown
   * @throws IOException  if a non-Alluxio exception occurs
   */
  private void deleteTmpFile() throws AlluxioException, IOException {
    if (!mFileTempPath.equals("")) {
      mFs.delete(new AlluxioURI(mFileTempPath));
    }
  }

  /**
   * Commits temporary file by transferring file from temporary dir to data dir.
   * @throws AlluxioException if an unexpected Alluxio exception is thrown
   * @throws IOException  if a non-Alluxio exception occurs
   */
  private void commitTmpFile() throws AlluxioException, IOException {
    long beginOffset = mOffset;
    long endOffset = mOffset + mRecordNum - 1;
    String strFormat = "%0" + Integer.toString(AlluxioSinkConnectorConfig.OFFSET_LENGTH) + "d";
    String strFileName =
        mTopicPartition.topic() + "+" + mTopicPartition.partition() + "+" + String
          .format(strFormat, beginOffset) + "+" + String.format(strFormat, endOffset)
          + mFormat.getExtension();
    String strNewFilePath = mTopicPartitionPath + "/" + strFileName;
    mFs.rename(new AlluxioURI(mFileTempPath), new AlluxioURI(strNewFilePath));
    mFileOutStream = null;
    mFileTempPath = "";
    mOffset += mRecordNum;
    mRecordNum = 0;
    mLastRotationTime = System.currentTimeMillis();
  }

  /**
   * Judges file should be rotated, if yes, close the temporary file.
   *
   * @param now current time
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
   * Creates temporary file for writing kafka record.
   *
   * @throws AlluxioException if an unexpected Alluxio exception is thrown
   * @throws IOException      if a non-Alluxio exception occurs
   */
  private void createTmpFile() throws AlluxioException, IOException {
    UUID id = UUID.randomUUID();
    mFileTempPath = mTmpTopicPartitionPath + "/" + id.toString() + "_tmp" + mFormat.getExtension();
    mFileOutStream = mFs.createFile(new AlluxioURI(mFileTempPath));
  }

  /**
   * Gets topic partition.
   *
   * @return topic partition
   */
  public TopicPartition getTopicPartition() {
    return mTopicPartition;
  }

  /**
   * Closes current topic partition writer.
   * @throws IOException  if a non-Alluxio exception occurs
   * @throws AlluxioException if an unexpected Alluxio exception is thrown
   */
  public void close() throws IOException, AlluxioException {
    closeTmpFile();
    deleteTmpFile();
  }

  private static enum State {
    WRITE_STARTED,
    WRITE_PARTITION_PAUSED,
    SHOULD_ROTATE,
    TEMPFILE_CLOSED,
    FILE_COMMITTED
  }
}
