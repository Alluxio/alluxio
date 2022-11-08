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

import alluxio.retry.RetryPolicy;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.AbortMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.PartETag;
import com.aliyun.oss.model.UploadPartRequest;
import com.aliyun.oss.model.UploadPartResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Aliyun OSS multipart upload helper.
 */
public class OSSMultipartUploadHelper {

  private static final Logger LOG = LoggerFactory.getLogger(OSSMultipartUploadHelper.class);

  /**
   * The request of each part for OSS multipart upload.
   */
  public static class FileHoldUploadPartRequest {
    private final String mBucketName;
    private final String mKey;
    private final String mUploadId;
    private final int mPartNumber;
    private final File mUploadFile;
    private final long mPartSize;

    /**
     * Constructs a new instance of {@link FileHoldUploadPartRequest}.
     * @param bucketName OSS bucket name
     * @param key OSS relative path
     * @param uploadId OSS upload id for multipart upload
     * @param partNumber part number for OSS multipart upload
     * @param uploadFile the file of this part for OSS multipart upload
     * @param partSize the part size
     */
    public FileHoldUploadPartRequest(String bucketName, String key, String uploadId,
                                     int partNumber, File uploadFile, long partSize) {
      mBucketName = bucketName;
      mKey = key;
      mUploadId = uploadId;
      mPartNumber = partNumber;
      mUploadFile = uploadFile;
      mPartSize = partSize;
    }

    /**
     * Returns the bucket name.
     * @return bucket name
     */
    public String getBucketName() {
      return mBucketName;
    }

    /**
     * Returns the key.
     * @return key
     */
    public String getKey() {
      return mKey;
    }

    /**
     * Returns the upload id.
     * @return upload id
     */
    public String getUploadId() {
      return mUploadId;
    }

    /**
     * Returns the part number.
     * @return part number
     */
    public int getPartNumber() {
      return mPartNumber;
    }

    /**
     * Returns the upload file.
     * @return upload file
     */
    public File getUploadFile() {
      return mUploadFile;
    }

    /**
     * Returns the part size.
     * @return part size
     */
    public long getPartSize() {
      return mPartSize;
    }
  }

  /**
   * Abort this multipart upload.
   * @param ossClient OSS client
   * @param bucketName OSS bucket name
   * @param key OSS relative path
   * @param uploadId OSS upload id for multipart upload
   * @param retryPolicy retry policy
   * @throws IOException if failed to abort
   */
  public static void abortMultiPartUpload(
      OSS ossClient, String bucketName, String key, String uploadId, RetryPolicy retryPolicy)
      throws IOException {
    Exception lastException;
    do {
      try {
        ossClient.abortMultipartUpload(
            new AbortMultipartUploadRequest(bucketName, key, uploadId));
        LOG.warn("Aborted multipart upload for key {} and id '{}' to bucket {}",
            key, uploadId, bucketName);
        return;
      } catch (Exception e) {
        lastException = e;
      }
    } while (retryPolicy.attempt());
    String requestId = lastException instanceof OSSException
        ? ((OSSException) lastException).getRequestId() : "NULL";
    throw new IOException(String.format("generate exception when abort multipart upload id "
            + "bucket %s, file path %s, upload id. %s, request id %s",
        bucketName, key, uploadId, requestId), lastException);
  }

  /**
   * Complete this multipart upload.
   * @param bucketName OSS bucket name
   * @param key OSS relative path
   * @param uploadId OSS upload id for multipart upload
   * @param tags the part tag
   * @param ossClient OSS client
   * @param retryPolicy retry policy
   * @throws IOException if failed to complete
   */
  public static void completeMultiPartUpload(
      String bucketName, String key, String uploadId, List<PartETag> tags, OSS ossClient,
      RetryPolicy retryPolicy) throws IOException {
    Exception lastException;
    CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(
        bucketName, key, uploadId, tags);
    do {
      try {
        ossClient.completeMultipartUpload(completeRequest);
        LOG.debug("Completed multipart upload for key {} and id '{}' with {} partitions.",
            key, uploadId, tags.size());
        return;
      } catch (Exception e) {
        lastException = e;
      }
    } while (retryPolicy.attempt());
    String requestId = lastException instanceof OSSException
        ? ((OSSException) lastException).getRequestId() : "NULL";
    throw new IOException(String.format("generate exception when complete upload id "
            + "bucket %s, file path %s, part No. %s, request id %s",
        bucketName, key, uploadId, requestId), lastException);
  }

  /**
   * Init a multipart upload.
   * @param bucketName OSS bucket name
   * @param key OSS relative path
   * @param ossClient OSS client
   * @param retryPolicy retry policy
   * @return upload id
   * @throws IOException if failed to init
   */
  public static String initMultiPartUpload(
      String bucketName, String key, OSS ossClient, RetryPolicy retryPolicy)
      throws IOException {
    InitiateMultipartUploadRequest initiateMultipartUploadRequest =
        new InitiateMultipartUploadRequest(bucketName, key);
    Exception lastException;
    do {
      try {
        return ossClient.initiateMultipartUpload(initiateMultipartUploadRequest).getUploadId();
      } catch (Exception e) {
        lastException = e;
      }
    } while (retryPolicy.attempt());
    String requestId = lastException instanceof OSSException
        ? ((OSSException) lastException).getRequestId() : "NULL";
    throw new IOException(String.format("generate exception when get upload id"
            + " bucket %s, file path %s, request id %s",
        bucketName, key, requestId), lastException);
  }

  /**
   * Upload part.
   * @param request upload request
   * @param mOssClient OSS client
   * @param retryPolicy retry policy
   * @return part ETag
   * @throws IOException if failed to upload
   */
  public static PartETag uploadPartFiles(
      OSSMultipartUploadHelper.FileHoldUploadPartRequest request, OSS mOssClient,
      RetryPolicy retryPolicy) throws IOException {
    PartETag partETag;
    Exception lastException;
    do {
      try (InputStream inputStream = new FileInputStream(request.getUploadFile())) {
        UploadPartRequest uploadPartRequest = new UploadPartRequest(
            request.getBucketName(), request.getKey(), request.getUploadId(),
            request.getPartNumber(), inputStream, request.getPartSize());
        UploadPartResult uploadPartResult = mOssClient.uploadPart(uploadPartRequest);
        partETag = uploadPartResult.getPartETag();
        return partETag;
      } catch (Exception e) {
        lastException = e;
      }
    } while (retryPolicy.attempt());
    String requestId = lastException instanceof OSSException
        ? ((OSSException) lastException).getRequestId() : "NULL";
    throw new IOException(String.format("generate exception when get upload id"
            + " bucket %s, file path %s, part No. %s, request id %s",
        request.getBucketName(), request.getKey(), request.getPartNumber(), requestId),
        lastException);
  }
}
