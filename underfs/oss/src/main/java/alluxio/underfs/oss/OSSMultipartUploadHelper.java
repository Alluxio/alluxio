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
import com.aliyun.oss.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class OSSMultipartUploadHelper {
    
    private static final Logger LOG = LoggerFactory.getLogger(OSSMultipartUploadHelper.class);
    
    public static class FileHoldUploadPartRequest {
        private final String bucketName;
        private final String key;
        private final String uploadId;
        private final int partNumber;
        private final File uploadFile;
        private final long partSize;

        public FileHoldUploadPartRequest(String bucketName, String key, String uploadId,
                                         int partNumber, File uploadFile, long partSize) {
            this.bucketName = bucketName;
            this.key = key;
            this.uploadId = uploadId;
            this.partNumber = partNumber;
            this.uploadFile = uploadFile;
            this.partSize = partSize;
        }

        public String getBucketName() {
            return bucketName;
        }

        public String getKey() {
            return key;
        }

        public String getUploadId() {
            return uploadId;
        }

        public int getPartNumber() {
            return partNumber;
        }

        public File getUploadFile() {
            return uploadFile;
        }

        public long getPartSize() {
            return partSize;
        }
    }

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
        String requestId = lastException instanceof OSSException ?
                ((OSSException) lastException).getRequestId() : "NULL";
        throw new IOException(String.format("generate exception when abort multipart upload id " +
                "bucket %s, file path %s, upload id. %s, request id %s",
                bucketName, key, uploadId, requestId), lastException);
    }

    public static void completeMultiPartUpload(
            String bucketName, String key, String mUploadId, List<PartETag> mTags, OSS mOssClient,
            RetryPolicy retryPolicy) throws IOException {
        Exception lastException;
        CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(
                bucketName, key, mUploadId, mTags);
        do {
            try {
                mOssClient.completeMultipartUpload(completeRequest);
                LOG.debug("Completed multipart upload for key {} and id '{}' with {} partitions.",
                        key, mUploadId, mTags.size());
                return;
            } catch (Exception e) {
                lastException = e;
            }
        } while (retryPolicy.attempt());
        String requestId = lastException instanceof OSSException ?
                ((OSSException) lastException).getRequestId() : "NULL";
        throw new IOException(String.format("generate exception when complete upload id " +
                "bucket %s, file path %s, part No. %s, request id %s",
                bucketName, key, mUploadId, requestId), lastException);
    }

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
        String requestId = lastException instanceof OSSException ?
                ((OSSException) lastException).getRequestId() : "NULL";
        throw new IOException(String.format("generate exception when get upload id" +
                " bucket %s, file path %s, request id %s",
                bucketName, key, requestId), lastException);
    }

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
        String requestId = lastException instanceof OSSException ?
                ((OSSException) lastException).getRequestId() : "NULL";
        throw new IOException(String.format("generate exception when get upload id" +
                " bucket %s, file path %s, part No. %s, request id %s",
                request.getBucketName(), request.getKey(), request.getPartNumber(), requestId),
                lastException);
    }
}
