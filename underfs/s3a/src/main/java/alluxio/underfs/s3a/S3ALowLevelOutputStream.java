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

package alluxio.underfs.s3a;

import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.internal.Mimetypes;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.util.Base64;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * [Experimental] A stream for writing a file into S3 using streaming upload.
 * The data transfer is done using S3 low-level multipart upload.
 *
 * The multipart upload is initialized in the first write() and an upload id is given
 * by AWS S3 to distinguish different multipart uploads.
 *
 * We upload data in partitions. When write(), the data will be persisted to
 * a temporary file {@link #mFile} on the local disk. When the data {@link #mPartitionOffset}
 * in this temporary file reaches the {@link #mPartitionSize}, the file will be submitted
 * to the upload executor {@link #mExecutor} and we do not wait for uploads to finish.
 * A new temp file will be created for the future write and the {@link #mPartitionOffset}
 * will be reset to zero. The process goes until all the data has been written to temp files.
 *
 * In flush(), we upload the buffered data if they are bigger than 5MB
 * and wait for all uploads to finish. The temp files will be deleted after uploading successfully.
 *
 * In close(), we upload the last part of data (if exists), wait for all uploads to finish,
 * and complete the multipart upload.
 *
 * close() will not be retried, but all the multipart upload
 * related operations(init, upload, complete, and abort) will be retried.
 *
 * If an error occurs and we have no way to recover, we abort the multipart uploads.
 * Some multipart uploads may not be completed/aborted in normal ways and need periodical cleanup
 * by enabling the {@link PropertyKey#UNDERFS_CLEANUP_ENABLED}.
 * When a leader master starts or a cleanup interval is reached, all the multipart uploads
 * older than {@link PropertyKey#UNDERFS_S3_INTERMEDIATE_UPLOAD_CLEAN_AGE} will be cleaned.
 */
@NotThreadSafe
public class S3ALowLevelOutputStream extends OutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(S3ALowLevelOutputStream.class);

  private final boolean mSseEnabled;

  private final List<String> mTmpDirs;

  /**
   * Only parts bigger than 5MB could be uploaded through S3A low-level multipart upload,
   * except the last part.
   */
  private static final long UPLOAD_THRESHOLD = 5L * Constants.MB;

  /** Bucket name of the Alluxio S3 bucket. */
  private final String mBucketName;

  /** The Amazon S3 client to interact with S3. */
  private final AmazonS3 mClient;

  /** Executing the upload tasks. */
  private final ListeningExecutorService mExecutor;

  /** Key of the file when it is uploaded to S3. */
  private final String mKey;

  /** The retry policy of this multipart upload. */
  private final RetryPolicy mRetryPolicy = new CountingRetry(5);

  /** Pre-allocated byte buffer for writing single characters. */
  private final byte[] mSingleCharWrite = new byte[1];

  /** Tags for the uploaded part, provided by S3 after uploading. */
  private final List<PartETag> mTags = new ArrayList<>();

  /** The MD5 hash of the file. */
  private MessageDigest mHash;

  /** The upload id of this multipart upload. */
  private String mUploadId;

  /** Flag to indicate this stream has been closed, to ensure close is only done once. */
  private boolean mClosed = false;

  /** When the offset reaches the partition size, we upload the temp file. */
  private long mPartitionOffset;
  /** The maximum allowed size of a partition. */
  private final long mPartitionSize;

  /**
   * The local temp file that will be uploaded when reaches the partition size
   * or when flush() is called and this file is bigger than 5MB.
   */
  private File mFile;
  /** The output stream to the local temp file. */
  private OutputStream mLocalOutputStream;

  /**
   * Give each upload request an unique and continuous id
   * so that S3 knows the part sequence to concatenate the parts to a single object.
   */
  private AtomicInteger mPartNumber;

  /** Store the future of tags. */
  private List<ListenableFuture<PartETag>> mTagFutures = new ArrayList<>();

  /**
   * Constructs a new stream for writing a file.
   *
   * @param bucketName the name of the bucket
   * @param key the key of the file
   * @param s3Client the Amazon S3 client to upload the file with
   * @param executor a thread pool executor
   * @param streamingUploadPartitionSize the size in bytes for partitions of streaming uploads
   * @param tmpDirs a list of temporary directories
   * @param sseEnabled whether or not server side encryption is enabled
   */
  public S3ALowLevelOutputStream(String bucketName, String key, AmazonS3 s3Client,
      ListeningExecutorService executor, long streamingUploadPartitionSize, List<String> tmpDirs,
      boolean sseEnabled) {
    Preconditions.checkArgument(bucketName != null && !bucketName.isEmpty(), "Bucket name must "
        + "not be null or empty.");
    mBucketName = bucketName;
    mClient = s3Client;
    mExecutor = executor;
    mTmpDirs = tmpDirs;
    mSseEnabled = sseEnabled;
    try {
      mHash = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      LOG.warn("Algorithm not available for MD5 hash.", e);
      mHash = null;
    }
    mKey = key;
    // Partition size should be at least 5 MB, since S3 low-level multipart upload does not
    // accept intermediate part smaller than 5 MB.
    mPartitionSize = Math.max(UPLOAD_THRESHOLD, streamingUploadPartitionSize);
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
      initMultiPartUpload();
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
    if (mPartitionOffset > UPLOAD_THRESHOLD) {
      uploadPart();
    }
    waitForAllPartsUpload();
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }

    // Set the closed flag, we never retry close() even if exception occurs
    mClosed = true;

    // Multi-part upload has not been initialized
    if (mUploadId == null) {
      LOG.debug("S3A Streaming upload output stream closed without uploading any data.");
      return;
    }

    try {
      if (mFile != null) {
        mLocalOutputStream.close();
        int partNumber = mPartNumber.getAndIncrement();
        final UploadPartRequest uploadRequest = new UploadPartRequest()
            .withBucketName(mBucketName)
            .withKey(mKey)
            .withUploadId(mUploadId)
            .withPartNumber(partNumber)
            .withFile(mFile)
            .withPartSize(mFile.length());
        uploadRequest.setLastPart(true);
        execUpload(uploadRequest);
      }

      waitForAllPartsUpload();
      completeMultiPartUpload();
    } catch (Exception e) {
      LOG.error("Failed to upload {}", mKey, e);
      throw new IOException(e);
    }
  }

  /**
   * Initializes multipart upload.
   */
  private void initMultiPartUpload() throws IOException {
    // Generate the object metadata by setting server side encryption, md5 checksum,
    // and encoding as octet stream since no assumptions are made about the file type
    ObjectMetadata meta = new ObjectMetadata();
    if (mSseEnabled) {
      meta.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
    }
    if (mHash != null) {
      meta.setContentMD5(Base64.encodeAsString(mHash.digest()));
    }
    meta.setContentType(Mimetypes.MIMETYPE_OCTET_STREAM);

    AmazonClientException lastException;
    InitiateMultipartUploadRequest initRequest =
        new InitiateMultipartUploadRequest(mBucketName, mKey).withObjectMetadata(meta);
    do {
      try {
        mUploadId = mClient.initiateMultipartUpload(initRequest).getUploadId();
        return;
      } catch (AmazonClientException e) {
        lastException = e;
      }
    } while (mRetryPolicy.attempt());
    // This point is only reached if the operation failed more
    // than the allowed retry count
    throw new IOException("Unable to init multipart upload to " + mKey, lastException);
  }

  /**
   * Creates a new temp file to write to.
   */
  private void initNewFile() throws IOException {
    mFile = new File(PathUtils.concatPath(CommonUtils.getTmpDir(mTmpDirs), UUID.randomUUID()));
    if (mHash != null) {
      mLocalOutputStream =
          new BufferedOutputStream(new DigestOutputStream(new FileOutputStream(mFile), mHash));
    } else {
      mLocalOutputStream = new BufferedOutputStream(new FileOutputStream(mFile));
    }
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
    UploadPartRequest uploadRequest = new UploadPartRequest()
        .withBucketName(mBucketName)
        .withKey(mKey)
        .withUploadId(mUploadId)
        .withPartNumber(partNumber)
        .withFile(newFileToUpload)
        .withPartSize(newFileToUpload.length());
    execUpload(uploadRequest);
  }

  /**
   * Executes the upload part request.
   *
   * @param request the upload part request
   */
  private void execUpload(UploadPartRequest request) {
    File file = request.getFile();
    ListenableFuture<PartETag> futureTag =
        mExecutor.submit((Callable) () -> {
          PartETag partETag;
          AmazonClientException lastException;
          try {
            do {
              try {
                partETag = mClient.uploadPart(request).getPartETag();
                return partETag;
              } catch (AmazonClientException e) {
                lastException = e;
              }
            } while (mRetryPolicy.attempt());
          } finally {
            // Delete the uploaded or failed to upload file
            if (!file.delete()) {
              LOG.error("Failed to delete temporary file @ {}", file.getPath());
            }
          }
          throw new IOException("Fail to upload part " + request.getPartNumber()
              + " to " + request.getKey(), lastException);
        });
    mTagFutures.add(futureTag);
    LOG.debug("Submit upload part request. key={}, partNum={}, file={}, fileSize={}, lastPart={}.",
        mKey, request.getPartNumber(), file.getPath(), file.length(), request.isLastPart());
  }

  /**
   * Waits for the submitted upload tasks to finish.
   */
  private void waitForAllPartsUpload() throws IOException {
    int beforeSize = mTags.size();
    try {
      for (ListenableFuture<PartETag> future : mTagFutures) {
        mTags.add(future.get());
      }
    } catch (ExecutionException e) {
      // No recover ways so that we need to cancel all the upload tasks
      // and abort the multipart upload
      Futures.allAsList(mTagFutures).cancel(true);
      abortMultiPartUpload();
      throw new IOException("Part upload failed in multipart upload with "
          + "id '" + mUploadId + "' to " + mKey, e);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted object upload.", e);
      Futures.allAsList(mTagFutures).cancel(true);
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
    AmazonClientException lastException;
    CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(mBucketName,
        mKey, mUploadId, mTags);
    do {
      try {
        mClient.completeMultipartUpload(completeRequest);
        LOG.debug("Completed multipart upload for key {} and id '{}' with {} partitions.",
            mKey, mUploadId, mTags.size());
        return;
      } catch (AmazonClientException e) {
        lastException = e;
      }
    } while (mRetryPolicy.attempt());
    // This point is only reached if the operation failed more
    // than the allowed retry count
    throw new IOException("Unable to complete multipart upload with id '"
        + mUploadId + "' to " + mKey, lastException);
  }

  /**
   * Aborts multipart upload.
   */
  private void abortMultiPartUpload() {
    AmazonClientException lastException;
    do {
      try {
        mClient.abortMultipartUpload(new AbortMultipartUploadRequest(mBucketName,
            mKey, mUploadId));
        LOG.warn("Aborted multipart upload for key {} and id '{}' to bucket {}",
            mKey, mUploadId, mBucketName);
        return;
      } catch (AmazonClientException e) {
        lastException = e;
      }
    } while (mRetryPolicy.attempt());
    // This point is only reached if the operation failed more
    // than the allowed retry count
    LOG.warn("Unable to abort multipart upload for key '{}' and id '{}' to bucket {}. "
        + "You may need to enable the periodical cleanup by setting property {}"
        + "to be true.", mKey, mUploadId, mBucketName,
        PropertyKey.UNDERFS_CLEANUP_ENABLED.getName(),
        lastException);
  }

  /**
   * Validates the arguments of write operation.
   *
   * @param b the data
   * @param off the start offset in the data
   * @param len the number of bytes to write
   */
  private void validateWriteArgs(byte[] b, int off, int len) {
    Preconditions.checkNotNull(b);
    if (off < 0 || off > b.length || len < 0
        || (off + len) > b.length || (off + len) < 0) {
      throw new IndexOutOfBoundsException("write(b[" + b.length + "], " + off + ", " + len + ")");
    }
  }
}
