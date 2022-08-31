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

package alluxio.underfs.obs;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.underfs.ObjectLowLevelOutputStream;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.obs.services.IObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.AbortMultipartUploadRequest;
import com.obs.services.model.CompleteMultipartUploadRequest;
import com.obs.services.model.InitiateMultipartUploadRequest;
import com.obs.services.model.ObjectMetadata;
import com.obs.services.model.PartEtag;
import com.obs.services.model.PutObjectRequest;
import com.obs.services.model.UploadPartRequest;
import com.obs.services.model.UploadPartResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * {@link ObjectLowLevelOutputStream} implement for OBS.
 */
public class OBSLowLevelOutputStream extends ObjectLowLevelOutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(OBSLowLevelOutputStream.class);

  /** The OBS client to interact with OBS. */
  private final IObsClient mClient;

  /** Tags for the uploaded part, provided by OBS after uploading. */
  private final List<PartEtag> mTags =
      Collections.synchronizedList(new ArrayList<>());

  /**
   * The upload id of this multipart upload.
   */
  protected volatile String mUploadId;

  /**
   * Constructs a new stream for writing a file.
   *
   * @param bucketName the name of the bucket
   * @param key the key of the file
   * @param obsClient the OBS client to upload the file with
   * @param executor a thread pool executor
   * @param ufsConf the object store under file system configuration
   */
  public OBSLowLevelOutputStream(
      String bucketName,
      String key,
      IObsClient obsClient,
      ListeningExecutorService executor,
      AlluxioConfiguration ufsConf) {
    super(bucketName, key, executor,
        ufsConf.getBytes(PropertyKey.UNDERFS_OBS_STREAMING_UPLOAD_PARTITION_SIZE), ufsConf);
    Preconditions.checkArgument(bucketName != null && !bucketName.isEmpty(),
        "Bucket name must not be null or empty.");
    mClient = obsClient;
  }

  @Override
  protected void uploadPartInternal(File file, int partNumber, boolean isLastPart, String md5)
      throws IOException {
    try {
      final UploadPartRequest uploadRequest = new UploadPartRequest();
      uploadRequest.setBucketName(mBucketName);
      uploadRequest.setObjectKey(mKey);
      uploadRequest.setUploadId(mUploadId);
      uploadRequest.setPartNumber(partNumber);
      uploadRequest.setFile(file);
      uploadRequest.setPartSize(file.length());
      if (md5 != null) {
        uploadRequest.setContentMd5(md5);
      }
      UploadPartResult result = getClient().uploadPart(uploadRequest);
      mTags.add(new PartEtag(result.getEtag(), result.getPartNumber()));
    } catch (ObsException e) {
      LOG.debug("failed to upload part.", e);
      throw new IOException(String.format(
          "failed to upload part. key: %s part number: %s uploadId: %s",
          mKey, partNumber, mUploadId), e);
    }
  }

  @Override
  protected void initMultiPartUploadInternal() throws IOException {
    try {
      ObjectMetadata meta = new ObjectMetadata();
      InitiateMultipartUploadRequest request =
          new InitiateMultipartUploadRequest(mBucketName, mKey);
      request.setMetadata(meta);
      mUploadId = getClient().initiateMultipartUpload(request).getUploadId();
    } catch (ObsException e) {
      LOG.debug("failed to init multi part upload", e);
      throw new IOException("failed to init multi part upload", e);
    }
  }

  @Override
  protected void completeMultiPartUploadInternal() throws IOException {
    try {
      LOG.info("complete part {}", mUploadId);
      CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(
          mBucketName, mKey, mUploadId, mTags);
      getClient().completeMultipartUpload(completeRequest);
    } catch (ObsException e) {
      LOG.debug("failed to complete multi part upload", e);
      throw new IOException(
          String.format("failed to complete multi part upload, key: %s, upload id: %s",
              mKey, mUploadId) + e);
    }
  }

  @Override
  protected void abortMultiPartUploadInternal() throws IOException {
    try {
      AbortMultipartUploadRequest request =
          new AbortMultipartUploadRequest(mBucketName, mKey, mUploadId);
      getClient().abortMultipartUpload(request);
    } catch (ObsException e) {
      LOG.debug("failed to abort multi part upload", e);
      throw new IOException(
          String.format("failed to complete multi part upload, key: %s, upload id: %s", mKey,
              mUploadId), e);
    }
  }

  @Override
  protected boolean isMultiPartUploadInitialized() {
    return mUploadId != null;
  }

  @Override
  protected void createEmptyObject(String key) throws IOException {
    try {
      ObjectMetadata meta = new ObjectMetadata();
      meta.setContentLength(0L);
      PutObjectRequest request =
          new PutObjectRequest(mBucketName, key, new ByteArrayInputStream(new byte[0]));
      request.setMetadata(meta);
      getClient().putObject(request);
    } catch (ObsException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected void putObject(String key, File file, String md5) throws IOException {
    try {
      ObjectMetadata meta = new ObjectMetadata();
      meta.setContentLength(file.length());
      if (md5 != null) {
        meta.setContentMd5(md5);
      }
      PutObjectRequest request =
          new PutObjectRequest(mBucketName, key, file);
      request.setMetadata(meta);
      getClient().putObject(request);
    } catch (ObsException e) {
      throw new IOException(e);
    }
  }

  protected IObsClient getClient() {
    return mClient;
  }
}
