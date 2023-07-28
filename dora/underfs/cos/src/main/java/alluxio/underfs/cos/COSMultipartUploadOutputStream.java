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

package alluxio.underfs.cos;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.underfs.ObjectMultipartUploadOutputStream;

import com.amazonaws.SdkClientException;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.qcloud.cos.COS;
import com.qcloud.cos.model.AbortMultipartUploadRequest;
import com.qcloud.cos.model.CompleteMultipartUploadRequest;
import com.qcloud.cos.model.InitiateMultipartUploadRequest;
import com.qcloud.cos.model.ObjectMetadata;
import com.qcloud.cos.model.PartETag;
import com.qcloud.cos.model.PutObjectRequest;
import com.qcloud.cos.model.UploadPartRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Object storage multipart upload for COS.
 */
@NotThreadSafe
public class COSMultipartUploadOutputStream extends ObjectMultipartUploadOutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(COSMultipartUploadOutputStream.class);

  /**
   * The COS client to interact with COS.
   */
  private final COS mClient;
  /**
   * Tags for the uploaded part, provided by COS after uploading.
   */
  private final List<PartETag> mTags = Collections.synchronizedList(new ArrayList<>());

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
   * @param COSClient   the COS client to upload the file with
   * @param executor   a thread pool executor
   * @param ufsConf    the object store under file system configuration
   */
  public COSMultipartUploadOutputStream(
      String bucketName,
      String key,
      COS COSClient,
      ListeningExecutorService executor,
      AlluxioConfiguration ufsConf) {
    super(bucketName, key, executor,
        ufsConf.getBytes(PropertyKey.UNDERFS_COS_MULTIPART_UPLOAD_PARTITION_SIZE), ufsConf);
    mClient = Preconditions.checkNotNull(COSClient);
  }

  @Override
  protected void uploadPartInternal(
      byte[] buf,
      int partNumber,
      boolean isLastPart,
      long length)
      throws IOException {
    try {
      InputStream inputStream = new BufferedInputStream(
          new ByteArrayInputStream(buf, 0, (int) length));

      final UploadPartRequest uploadRequest = new UploadPartRequest()
          .withBucketName(mBucketName)
          .withKey(mKey)
          .withUploadId(mUploadId)
          .withPartNumber(partNumber)
          .withInputStream(inputStream)
          .withPartSize(length);

      // calculate md5 digest
      MessageDigest md = MessageDigest.getInstance("MD5");
      md.update(buf, 0, (int) length);

      // set parameter of md5 digest
      uploadRequest.setMd5Digest(Base64.getEncoder().encodeToString(md.digest()));

      // set parameter of isLastPart
      uploadRequest.setLastPart(isLastPart);

      // Upload this part
      PartETag partETag = getClient().uploadPart(uploadRequest).getPartETag();
      mTags.add(partETag);
    } catch (SdkClientException e) {
      LOG.debug("failed to upload part.", e);
      throw new IOException(String.format(
          "failed to upload part. key: %s part number: %s uploadId: %s",
          mKey, partNumber, mUploadId), e);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void initMultipartUploadInternal() throws IOException {
    try {
      ObjectMetadata meta = new ObjectMetadata();
      mUploadId = getClient()
          .initiateMultipartUpload(new InitiateMultipartUploadRequest(mBucketName, mKey, meta))
          .getUploadId();
    } catch (SdkClientException e) {
      LOG.debug("failed to init multi part upload", e);
      throw new IOException("failed to init multi part upload", e);
    }
  }

  @Override
  protected void completeMultipartUploadInternal() throws IOException {
    try {
      LOG.debug("complete multi part {}", mUploadId);
      mContentHash = getClient().completeMultipartUpload(new CompleteMultipartUploadRequest(
          mBucketName, mKey, mUploadId, mTags)).getETag();
    } catch (SdkClientException e) {
      LOG.debug("failed to complete multi part upload", e);
      throw new IOException(
          String.format("failed to complete multi part upload, key: %s, upload id: %s",
              mKey, mUploadId), e);
    }
  }

  @Override
  protected void abortMultipartUploadInternal() throws IOException {
    try {
      getClient().abortMultipartUpload(
          new AbortMultipartUploadRequest(mBucketName, mKey, mUploadId));
    } catch (SdkClientException e) {
      LOG.debug("failed to abort multi part upload", e);
      throw new IOException(
          String.format("failed to abort multi part upload, key: %s, upload id: %s", mKey,
              mUploadId), e);
    }
  }

  @Override
  protected void createEmptyObject(String key) throws IOException {
    try {
      ObjectMetadata meta = new ObjectMetadata();
      meta.setContentLength(0);
      mContentHash = getClient().putObject(
              new PutObjectRequest(mBucketName, key, new ByteArrayInputStream(new byte[0]), meta))
          .getETag();
    } catch (SdkClientException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected void putObject(String key, byte[] buf, long length) throws IOException {
    try {
      ObjectMetadata meta = new ObjectMetadata();

      InputStream inputStream = new BufferedInputStream(
          new ByteArrayInputStream(buf, 0, (int) length));

      // calculate md5 digest
      MessageDigest md = MessageDigest.getInstance("MD5");
      md.update(buf, 0, (int) length);

      // set parameter of md5 digest
      meta.setContentMD5(Base64.getEncoder().encodeToString(md.digest()));

      // set other parameters
      meta.setContentLength(length);

      // upload this file whose data is in the array buf.
      PutObjectRequest putReq = new PutObjectRequest(mBucketName, key, inputStream, meta);
      mContentHash = getClient().putObject(putReq).getETag();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  protected COS getClient() {
    return mClient;
  }

  @Override
  public Optional<String> getContentHash() {
    return Optional.ofNullable(mContentHash);
  }
}
