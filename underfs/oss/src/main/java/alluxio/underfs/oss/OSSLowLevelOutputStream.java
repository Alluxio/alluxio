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

package alluxio.underfs.oss;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.underfs.ObjectLowLevelOutputStream;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.internal.Mimetypes;
import com.aliyun.oss.model.AbortMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PartETag;
import com.aliyun.oss.model.PutObjectRequest;
import com.aliyun.oss.model.UploadPartRequest;
import com.google.common.util.concurrent.ListeningExecutorService;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * {@link ObjectLowLevelOutputStream} implement for OSS.
 */
public class OSSLowLevelOutputStream extends ObjectLowLevelOutputStream {
  /** The OSS client to interact with OSS. */
  private final OSS mClient;
  /** Tags for the uploaded part, provided by OSS after uploading. */
  private final List<PartETag> mTags =
      Collections.synchronizedList(new ArrayList<>());

  /** The upload id of this multipart upload. */
  protected volatile String mUploadId;

  /**
   * Constructs a new stream for writing a file.
   *
   * @param bucketName the name of the bucket
   * @param key the key of the file
   * @param oss the OSS client to upload the file with
   * @param executor a thread pool executor
   * @param ufsConf the object store under file system configuration
   */
  public OSSLowLevelOutputStream(
      String bucketName,
      String key,
      OSS oss,
      ListeningExecutorService executor,
      AlluxioConfiguration ufsConf) {
    super(bucketName, key, executor,
        ufsConf.getBytes(PropertyKey.UNDERFS_OSS_STREAMING_UPLOAD_PARTITION_SIZE), ufsConf);
    mClient = oss;
  }

  @Override
  protected void abortMultiPartUploadInternal() throws IOException {
    try {
      getClient().abortMultipartUpload(new AbortMultipartUploadRequest(mBucketName,
          mKey, mUploadId));
    } catch (OSSException | ClientException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected void uploadPartInternal(File file, int partNumber, boolean isLastPart, String md5)
      throws IOException {
    try {
      try (InputStream inputStream = new BufferedInputStream(new FileInputStream(file))) {
        final UploadPartRequest uploadRequest =
            new UploadPartRequest(mBucketName, mKey, mUploadId, partNumber, inputStream,
                file.length());
        if (md5 != null) {
          uploadRequest.setMd5Digest(md5);
        }
        PartETag partETag = getClient().uploadPart(uploadRequest).getPartETag();
        mTags.add(partETag);
      }
    } catch (OSSException | ClientException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected void initMultiPartUploadInternal() throws IOException {
    try {
      ObjectMetadata meta = new ObjectMetadata();
      meta.setContentType(Mimetypes.DEFAULT_MIMETYPE);
      InitiateMultipartUploadRequest initRequest =
          new InitiateMultipartUploadRequest(mBucketName, mKey, meta);
      mUploadId = getClient().initiateMultipartUpload(initRequest).getUploadId();
    } catch (OSSException | ClientException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected void completeMultiPartUploadInternal() throws IOException {
    try {
      CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(
          mBucketName, mKey, mUploadId, mTags);
      getClient().completeMultipartUpload(completeRequest);
    } catch (OSSException | ClientException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected boolean isMultiPartUploadInitialized() {
    return mUploadId != null;
  }

  @Override
  protected void createEmptyObject(String key) throws IOException {
    try {
      ObjectMetadata objMeta = new ObjectMetadata();
      objMeta.setContentLength(0);
      getClient().putObject(mBucketName, key, new ByteArrayInputStream(new byte[0]), objMeta);
    } catch (OSSException | ClientException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected void putObject(String key, File file, String md5) throws IOException {
    try {
      ObjectMetadata objMeta = new ObjectMetadata();
      if (md5 != null) {
        objMeta.setContentMD5(md5);
      }
      PutObjectRequest request = new PutObjectRequest(mBucketName, key, file, objMeta);
      getClient().putObject(request);
    } catch (OSSException | ClientException e) {
      throw new IOException(e);
    }
  }

  protected OSS getClient() {
    return mClient;
  }
}
