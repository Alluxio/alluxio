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

import alluxio.Constants;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;
import alluxio.util.io.PathUtils;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.PartETag;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Stream for writing a file into OSS by multipart upload.
 */
public class MultipartOSSOutputStream extends OutputStream {

  private static final Logger LOG = LoggerFactory.getLogger(MultipartOSSOutputStream.class);
  private static final long UPLOAD_THRESHOLD = 5L * Constants.MB;

  private final String mKey;
  private final String mBucketName;
  private final OSS mOssClient;
  private final long mPartitionSize;
  private final List<String> mTmpDirs;
  private OutputStream mLocalOutputStream;
  private String mUploadId;
  private final AtomicInteger mPartNumber;
  private List<Future<PartETag>> mTagFutures = new ArrayList<>();
  private final ExecutorService mExecutor;
  private long mPartitionOffset;
  private final AtomicBoolean mClosed = new AtomicBoolean(false);
  private final byte[] mSingleCharWrite = new byte[1];
  private File mFile;
  private final List<PartETag> mTags = new ArrayList<>();
  private final RetryPolicy mRetryPolicy = new CountingRetry(3);

  /**
   * Creates a name instance of {@link OSSOutputStream}.
   *
   * @param bucketName the name of the bucket
   * @param key        the key of the file
   * @param client     the client for OSS
   * @param tmpDirs    a list of temporary directories
   * @param uploadPartitionSize the part size
   * @param poolSize the thread pool size
   */
  public MultipartOSSOutputStream(String bucketName, String key, OSS client, List<String> tmpDirs,
                                  long uploadPartitionSize, int poolSize) {
    Preconditions.checkArgument(bucketName != null && !bucketName.isEmpty(),
        "Bucket name must not be null or empty.");
    Preconditions.checkArgument(key != null && !key.isEmpty(),
        "OSS path must not be null or empty.");
    Preconditions.checkArgument(client != null, "OSSClient must not be null.");
    mBucketName = bucketName;
    mKey = key;
    mOssClient = client;
    mExecutor = Executors.newFixedThreadPool(poolSize);
    mTmpDirs = tmpDirs;
    // Partition size should be at least 5 MB, since S3 low-level multipart upload does not
    // accept intermediate part smaller than 5 MB.
    mPartitionSize = Math.max(UPLOAD_THRESHOLD, uploadPartitionSize);
    mPartNumber = new AtomicInteger(1);
  }

  @Override
  public void write(int b) throws IOException {
    mSingleCharWrite[0] = (byte) b;
    write(mSingleCharWrite);
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (b == null || len == 0) {
      return;
    }
    validateWriteArgs(b, off, len);
    if (mUploadId == null) {
      mUploadId = OSSMultipartUploadHelper.initMultiPartUpload(
          mBucketName, mKey, mOssClient, mRetryPolicy);
    }
    if (mFile == null) {
      initNewFile();
    }
    if (mPartitionOffset + len < mPartitionSize) {
      mLocalOutputStream.write(b, off, len);
      mPartitionOffset += len;
    } else {
      int firstLen = (int) (mPartitionSize - mPartitionOffset);
      mLocalOutputStream.write(b, off, firstLen);
      mPartitionOffset += firstLen;
      uploadPart();
      write(b, off + firstLen, len - firstLen);
    }
  }

  @Override
  public void flush() throws IOException {
    if (mUploadId == null) {
      return;
    }
    // We try to minimize the time use to close()
    // because Fuse release() method which calls close() is async.
    // In flush(), we upload the current writing file if it is bigger than 5 MB,
    // and wait for all current upload to complete.
    if (mLocalOutputStream != null) {
      mLocalOutputStream.flush();
    }
  }

  @Override
  public void close() throws IOException {
    if (mClosed.getAndSet(true)) {
      return;
    }

    // Multipart upload has not been initialized
    try {
      if (mUploadId == null) {
        LOG.info("OSS Streaming upload output stream closed without uploading any data.");
        String tmpDir = prepareTmpDir();
        File emptyFile = new File(PathUtils.concatPath(tmpDir, UUID.randomUUID()));
        Files.newOutputStream(emptyFile.toPath()).close();
        try (BufferedInputStream in = new BufferedInputStream(
            Files.newInputStream(emptyFile.toPath()))) {
          mOssClient.putObject(mBucketName, mKey, in);
        } finally {
          emptyFile.delete();
        }
        return;
      }

      try {
        if (mFile != null) {
          mLocalOutputStream.close();
          int partNumber = mPartNumber.getAndIncrement();
          final OSSMultipartUploadHelper.FileHoldUploadPartRequest uploadRequest =
              new OSSMultipartUploadHelper.FileHoldUploadPartRequest(
                  mBucketName, mKey, mUploadId, partNumber, mFile, mFile.length());
          execUpload(uploadRequest, mExecutor);
        }

        waitForAllPartsUpload();
        completeMultiPartUpload();
      } catch (Exception e) {
        LOG.error("Failed to upload {}", mKey, e);
        throw new IOException(e);
      }
    } finally {
      mExecutor.shutdownNow();
    }
  }

  private String prepareTmpDir() throws IOException {
    String tmpDir = mTmpDirs.get(0);
    String dir = PathUtils.concatPath(tmpDir, mKey);
    Path dirPath = Paths.get(dir);
    if (!Files.exists(dirPath)) {
      Files.createDirectories(dirPath);
    }
    return dir;
  }

  /**
   * Creates a new temp file to write to.
   */
  private void initNewFile() throws IOException {
    String tmpDir = prepareTmpDir();
    mFile = new File(PathUtils.concatPath(tmpDir, UUID.randomUUID()));
    mLocalOutputStream = new BufferedOutputStream(Files.newOutputStream(mFile.toPath()));
    mPartitionOffset = 0;
    LOG.debug("Init new temp file @ {}", mFile.getPath());
  }

  /**
   * Uploads part async.
   */
  private void uploadPart() throws IOException {
    if (mFile == null) {
      return;
    }
    mLocalOutputStream.close();
    int partNumber = mPartNumber.getAndIncrement();
    File newFileToUpload = new File(mFile.getPath());
    mFile = null;
    mLocalOutputStream = null;
    OSSMultipartUploadHelper.FileHoldUploadPartRequest uploadRequest;
    try {
      uploadRequest = new OSSMultipartUploadHelper.FileHoldUploadPartRequest(
          mBucketName, mKey, mUploadId, partNumber, newFileToUpload,
          newFileToUpload.length());
      execUpload(uploadRequest, mExecutor);
    } catch (Exception e) {
      throw new IOException(String.format("generate exception when submit upload task "
          + "bucket %s, file path %s, part No. %s", mBucketName, mKey, partNumber), e);
    }
  }

  /**
   * Executes the upload part request.
   *
   * @param request   the upload part request
   * @param mExecutor
   */
  private void execUpload(OSSMultipartUploadHelper.FileHoldUploadPartRequest request,
                          ExecutorService mExecutor) {
    Future<PartETag> futureTag =
        mExecutor.submit(() -> {
          try {
            return OSSMultipartUploadHelper.uploadPartFiles(
                request, mOssClient, mRetryPolicy);
          } finally {
            // Delete the uploaded or failed to upload file
            if (!request.getUploadFile().delete()) {
              LOG.error("Failed to delete temporary file @ {}",
                  request.getUploadFile().getPath());
            }
          }
        });
    mTagFutures.add(futureTag);
  }

  /**
   * Waits for the submitted upload tasks to finish.
   */
  private void waitForAllPartsUpload() throws IOException {
    int beforeSize = mTags.size();
    try {
      for (Future<PartETag> future : mTagFutures) {
        mTags.add(future.get());
      }
    } catch (ExecutionException e) {
      // No recover ways so that we need to cancel all the upload tasks
      // and abort the multipart upload
      mTagFutures.forEach(partETagFuture -> partETagFuture.cancel(true));
      abortMultiPartUpload();
      throw new IOException("Part upload failed in multipart upload with "
          + "id '" + mUploadId + "' to " + mKey, e);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted object upload.", e);
      mTagFutures.forEach(partETagFuture -> partETagFuture.cancel(true));
      abortMultiPartUpload();
      Thread.currentThread().interrupt();
    }
    mTagFutures = new ArrayList<>();
    if (mTags.size() != beforeSize) {
      LOG.debug("Uploaded {} partitions of id '{}' to {}.", mTags.size(), mUploadId, mKey);
    }
  }

  /**
   * Completes multipart upload.
   */
  private void completeMultiPartUpload() throws IOException {
    OSSMultipartUploadHelper.completeMultiPartUpload(
        mBucketName, mKey, mUploadId, mTags, mOssClient, mRetryPolicy);
  }

  /**
   * Aborts multipart upload.
   */
  private void abortMultiPartUpload() throws IOException {
    OSSMultipartUploadHelper.abortMultiPartUpload(
        mOssClient, mBucketName, mKey, mUploadId, mRetryPolicy);
  }

  /**
   * Validates the arguments of write operation.
   *
   * @param b   the data
   * @param off the start offset in the data
   * @param len the number of bytes to write
   */
  private void validateWriteArgs(byte[] b, int off, int len) {
    Preconditions.checkNotNull(b);
    if (off < 0 || off > b.length || len < 0
        || (off + len) > b.length || (off + len) < 0) {
      throw new IndexOutOfBoundsException(
          "write(b[" + b.length + "], " + off + ", " + len + ")");
    }
  }
}
