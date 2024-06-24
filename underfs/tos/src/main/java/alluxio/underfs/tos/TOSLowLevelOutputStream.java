/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.underfs.tos;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.underfs.ObjectLowLevelOutputStream;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.volcengine.tos.TOSV2;
import com.volcengine.tos.TosException;
import com.volcengine.tos.comm.io.TosRepeatableBoundedFileInputStream;
import com.volcengine.tos.model.object.AbortMultipartUploadInput;
import com.volcengine.tos.model.object.CompleteMultipartUploadV2Input;
import com.volcengine.tos.model.object.CompleteMultipartUploadV2Output;
import com.volcengine.tos.model.object.CreateMultipartUploadInput;
import com.volcengine.tos.model.object.CreateMultipartUploadOutput;
import com.volcengine.tos.model.object.PutObjectInput;
import com.volcengine.tos.model.object.UploadPartV2Input;
import com.volcengine.tos.model.object.UploadPartV2Output;
import com.volcengine.tos.model.object.UploadedPartV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Object storage low output stream for TOS.
 */
@NotThreadSafe
public class TOSLowLevelOutputStream extends ObjectLowLevelOutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(TOSLowLevelOutputStream.class);

  /**
   * The TOS client to interact with TOS.
   */
  protected TOSV2 mClient;
  /**
   * Tags for the uploaded part, provided by TOS after uploading.
   */
  private final List<UploadedPartV2> mTags = Collections.synchronizedList(new ArrayList<>());

  /**
   * The upload id of this multipart upload.
   */
  protected volatile String mUploadId;

  private String mContentHash;

  /**
   * Constructs a new stream for writing a file.
   *
   * @param bucketName the name of the bucket
   * @param key        the key of the file
   * @param tosClient  the TOS client to upload the file with
   * @param executor   a thread pool executor
   * @param ufsConf    the object store under file system configuration
   */
  public TOSLowLevelOutputStream(String bucketName, String key, TOSV2 tosClient,
                                 ListeningExecutorService executor,
                                 AlluxioConfiguration ufsConf) {
    super(bucketName, key, executor,
        ufsConf.getBytes(PropertyKey.UNDERFS_TOS_STREAMING_UPLOAD_PARTITION_SIZE), ufsConf);
    mClient = Preconditions.checkNotNull(tosClient);
  }

  @Override
  protected void abortMultiPartUploadInternal() throws IOException {
    try {
      AbortMultipartUploadInput input = new AbortMultipartUploadInput().setBucket(mBucketName)
          .setKey(mKey).setUploadID(mUploadId);
      getClient().abortMultipartUpload(input);
    } catch (TosException e) {
      LOG.debug("failed to abort multi part upload. upload id: {}", mUploadId, e);
      throw AlluxioTosException.from(String.format(
          "failed to upload part. key: %s uploadId: %s",
          mKey, mUploadId), e);
    }
  }

  @Override
  protected void uploadPartInternal(
      File file,
      int partNumber,
      boolean isLastPart,
      @Nullable String md5)
      throws IOException {
    long fileSize = file.length();
    long partSize = mPartitionSize;
    try (FileInputStream content = new FileInputStream(file)) {
      InputStream wrappedContent = new TosRepeatableBoundedFileInputStream(content, mPartitionSize);
      if (isLastPart) {
        partSize = fileSize;
      }
      final UploadPartV2Input input = new UploadPartV2Input()
          .setBucket(mBucketName)
          .setKey(mKey)
          .setUploadID(mUploadId)
          .setPartNumber(partNumber)
          .setContentLength(partSize)
          .setContent(wrappedContent);
      UploadPartV2Output output = getClient().uploadPart(input);
      mTags.add(new UploadedPartV2().setPartNumber(partNumber).setEtag(output.getEtag()));
    } catch (IOException e) {
      LOG.debug("failed to upload part.", e);
      throw new IOException(String.format(
          "failed to upload part. key: %s part number: %s uploadId: %s",
          mKey, partNumber, mUploadId), e);
    }
  }

  @Override
  protected void initMultiPartUploadInternal() throws IOException {
    try {
      CreateMultipartUploadInput create =
          new CreateMultipartUploadInput().setBucket(mBucketName).setKey(mKey);
      CreateMultipartUploadOutput output = getClient().createMultipartUpload(create);
      mUploadId = output.getUploadID();
    } catch (TosException e) {
      LOG.debug("failed to init multi part upload", e);
      throw AlluxioTosException.from("failed to init multi part upload", e);
    }
  }

  @Override
  protected void completeMultiPartUploadInternal() {
    try {
      LOG.debug("complete multi part {}", mUploadId);
      CompleteMultipartUploadV2Input complete = new CompleteMultipartUploadV2Input()
          .setBucket(mBucketName)
          .setKey(mKey)
          .setUploadID(mUploadId);
      complete.setUploadedParts(mTags);
      CompleteMultipartUploadV2Output completedOutput =
          getClient().completeMultipartUpload(complete);
      mContentHash = completedOutput.getEtag();
    } catch (TosException e) {
      LOG.debug("failed to complete multi part upload", e);
      throw AlluxioTosException.from(
          String.format("failed to complete multi part upload, key: %s, upload id: %s",
              mKey, mUploadId), e);
    }
  }

  @Override
  protected void createEmptyObject(String key) {
    try {
      PutObjectInput putObjectInput = new PutObjectInput()
          .setBucket(mBucketName)
          .setKey(key)
          .setContent(new ByteArrayInputStream(new byte[0]))
          .setContentLength(0);
      mContentHash = getClient().putObject(putObjectInput).getEtag();
    } catch (TosException e) {
      LOG.debug("failed to create empty object", e);
      throw AlluxioTosException.from(e);
    }
  }

  @Override
  protected void putObject(String key, File file, @Nullable String md5) throws IOException {
    try (InputStream content = Files.newInputStream(file.toPath())) {
      PutObjectInput putObjectInput = new PutObjectInput()
          .setBucket(mBucketName)
          .setKey(key)
          .setContent(content)
          .setContentLength(file.length()); // Set the correct content length
      mContentHash = getClient().putObject(putObjectInput).getEtag();
    } catch (TosException e) {
      LOG.debug("failed to put object", e);
      throw AlluxioTosException.from(e);
    }
  }

  @Override
  public Optional<String> getContentHash() {
    return Optional.ofNullable(mContentHash);
  }

  protected TOSV2 getClient() {
    return mClient;
  }
}
