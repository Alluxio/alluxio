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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
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
import com.google.common.util.concurrent.MoreExecutors;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * [Experimental] A stream for writing a file into S3 using streaming upload.
 * The data will be persisted to a temporary directory on the local disk
 * and upload when buffered data reaches the partition size. The data
 * transfer is done using a S3 low-level multipart upload API.
 */
@NotThreadSafe
public class S3ALowLevelOutputStream extends OutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(S3ALowLevelOutputStream.class);

  private static final boolean SSE_ENABLED =
      Configuration.getBoolean(PropertyKey.UNDERFS_S3A_SERVER_SIDE_ENCRYPTION_ENABLED);
  private static final long UPLOAD_THRESHOLD = 5 * Constants.MB;

  /** Bucket name of the Alluxio S3 bucket. */
  private final String mBucketName;

  /** The Amazon S3 client responsible to upload data to S3. */
  private final AmazonS3 mClient;

  /** Executing the upload tasks. */
  private final ListeningExecutorService mExecutor;

  /** Key of the file when it is uploaded to S3. */
  private final String mKey;

  /** Allowed size of the buffer file. */
  private final long mPartitionSize;

  /** Pre-allocated byte buffer for writing single characters. */
  private final byte[] mSingleCharWrite = new byte[1];

  /** Tags for the uploaded part, provided by S3 after uploading. */
  private final List<PartETag> mTags = new ArrayList<>();

  /** The retry policy of this multipart upload. */
  private final RetryPolicy mRetryPolicy = new CountingRetry(5);

  /** The upload id of this multipart upload. */
  private String mUploadId;

  /** Flag to indicate this stream has been closed, to ensure close is only done once. */
  private boolean mClosed = false;

  /** The MD5 hash of the file. */
  private MessageDigest mHash;

  /**
   * The local file that will be uploaded when reaches the partition size
   * or when flush() is called and this file is bigger than 5MB.
   */
  private File mFile;

  /** The output stream to a local file where the file will be buffered. */
  private OutputStream mLocalOutputStream;

  /** Store the future of tags. */
  private List<ListenableFuture<PartETag>> mFutureTags = new ArrayList<>();

  /**
   * Give each upload request an unique and continuous id
   * so that S3 knows the part sequence to concatenate the parts to a single object.
   */
  private AtomicInteger mPartNumber;

  /** When the offset reaches the partition size, we upload the temp file. */
  private long mOffset;

  /**
   * Constructs a new stream for writing a file.
   *
   * @param bucketName the name of the bucket
   * @param key the key of the file
   * @param s3Client the Amazon S3 client to upload the file with
   * @param executor a thread pool executor
   */
  public S3ALowLevelOutputStream(String bucketName, String key, AmazonS3 s3Client,
      ExecutorService executor) {
    Preconditions.checkArgument(bucketName != null && !bucketName.isEmpty(), "Bucket name must "
        + "not be null or empty.");
    // Partition size should be at least 5 MB, since S3 low-level multipart upload does not
    // accept intermediate part smaller than 5 MB.
    long partSize = Configuration.getBytes(PropertyKey.UNDERFS_S3A_STREAMING_UPLOAD_PARTITION_SIZE);
    mPartitionSize = partSize < 5 * Constants.MB ? 5 * Constants.MB : partSize;
    mBucketName = bucketName;
    mKey = key;
    mClient = s3Client;
    mExecutor = MoreExecutors.listeningDecorator(executor);
    try {
      mHash = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      LOG.warn("Algorithm not available for MD5 hash.", e);
      mHash = null;
    }

    // Generate the object metadata by setting server side encryption, md5 checksum,
    // and encoding as octet stream since no assumptions are made about the file type
    ObjectMetadata meta = new ObjectMetadata();
    if (SSE_ENABLED) {
      meta.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
    }
    if (mHash != null) {
      meta.setContentMD5(new String(Base64.encode(mHash.digest())));
    }
    meta.setContentType(Mimetypes.MIMETYPE_OCTET_STREAM);

    InitiateMultipartUploadRequest initRequest =
        new InitiateMultipartUploadRequest(mBucketName, mKey).withObjectMetadata(meta);
    do {
      try {
        mUploadId = s3Client.initiateMultipartUpload(initRequest).getUploadId();
        break;
      } catch (Exception e) {
        LOG.warn("Unable to init multipart upload " + e);
      }
    } while (mRetryPolicy.attempt());

    mPartNumber = new AtomicInteger(1);
    LOG.debug("Multi-part upload initiated. bucket={}, key={}, uploadId={}, partitionSize={}.",
        mBucketName, mKey, mUploadId, mPartitionSize);
  }

  @Override
  public void write(int b) throws IOException {
    mSingleCharWrite[0] = (byte) b;
    write(mSingleCharWrite, 0, 1);
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (b == null || len == 0) {
      return;
    } else if ((off < 0) || (off > b.length) || (len < 0)
        || ((off + len) > b.length) || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException();
    }
    if (mFile == null) {
      initNewFile();
    }
    if (mOffset + len < mPartitionSize) {
      mLocalOutputStream.write(b, off, len);
      mOffset += len;
    } else {
      int firstLen = (int) (mPartitionSize - mOffset);
      mLocalOutputStream.write(b, off, firstLen);
      mOffset += firstLen;
      uploadPart();
      write(b, off + firstLen, len - firstLen);
    }
  }

  @Override
  public void flush() throws IOException {
    // We try to minimize the time use to close()
    // because Fuse release() method which calls close() is async.
    // In flush(), we upload the current writing file if it is bigger than 5 MB,
    // and wait for all current upload to complete.
    mLocalOutputStream.flush();
    if (mOffset > UPLOAD_THRESHOLD) {
      uploadPart();
    }
    waitForAllPartsUpload();
    LOG.info("flush() : get tags {}", mTags);
    mFutureTags = new ArrayList<>();
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
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
        execUpload(uploadRequest, mFile, partNumber);
      }

      waitForAllPartsUpload();

      CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(mBucketName,
          mKey, mUploadId, mTags);
      do {
        try {
          mClient.completeMultipartUpload(compRequest);
          break;
        } catch (Exception e) {
          LOG.warn("Unable to complete multipart upload " + e);
        }
      } while (mRetryPolicy.attempt());

      LOG.debug("Completed multipart upload. partNum={}, bucketName={}, key={}.",
          mTags.size(), mBucketName, mKey);
    } catch (Exception e) {
      LOG.error("Failed to upload {}: {}", getUploadPath(), e.toString());
      throw new IOException(e);
    }

    // Set the closed flag, close can be retried until mFile.delete is called successfully
    mClosed = true;
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
    execUpload(uploadRequest, newFileToUpload, partNumber);
  }

  /**
   * Executes the upload part request.
   */
  private void execUpload(UploadPartRequest request, File file, int partNumber) {
    ListenableFuture<PartETag> futureTag =
        mExecutor.submit((Callable) () -> {
          PartETag partETag;
          try {
            partETag = mClient.uploadPart(request).getPartETag();
            LOG.debug("Completed upload of part {} and get ETag {}", partNumber,
                partETag.getETag());
            return partETag;
          } finally {
            // Delete the uploaded file
            file.delete();
          }
        });
    mFutureTags.add(futureTag);
    LOG.debug("Submit upload part request. partNum={}, file={}, fileSize={}.",
        partNumber, file.getPath(), file.length());
  }

  /**
   * Creates a new temp file to write to.
   */
  private void initNewFile() throws IOException {
    mFile = new File(PathUtils.concatPath(CommonUtils.getTmpDir(), UUID.randomUUID()));
    if (mHash != null) {
      mLocalOutputStream =
          new BufferedOutputStream(new DigestOutputStream(new FileOutputStream(mFile), mHash));
    } else {
      mLocalOutputStream = new BufferedOutputStream(new FileOutputStream(mFile));
    }
    mOffset = 0;
    LOG.debug("Init new temp file @ {}", mFile.getPath());
  }

  /**
   * Aborts multipart upload.
   */
  private void abortMultiPartUpload() {
    LOG.warn("Aborting multi-part upload with id '{}'", mUploadId);
    do {
      try {
        mClient.abortMultipartUpload(new AbortMultipartUploadRequest(mBucketName,
            mKey, mUploadId));
        return;
      } catch (AmazonClientException e) {
        // Ignore the exception and keep retrying
      }
    } while (mRetryPolicy.attempt());
    // This point is only reached if the operation failed more
    // than the allowed retry count
    LOG.warn("Unable to abort multipart upload with id '{}'.", mUploadId);
  }

  /**
   * Waits for the submitted upload tasks to finish.
   */
  private void waitForAllPartsUpload() throws IOException {
    try {
      mTags.addAll(Futures.allAsList(mFutureTags).get());
    } catch (ExecutionException e) {
      // No recover ways so that we need to cancel all the upload tasks
      // and abort the multipart upload
      for (ListenableFuture<PartETag> future : mFutureTags) {
        future.cancel(true);
      }
      abortMultiPartUpload();
      throw new IOException("Part upload failed in multi-part upload with "
          + "id '" + mUploadId + "':" + e);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted object upload:" + e);
      Thread.currentThread().interrupt();
    }
    LOG.debug("All upload requests finished.");
  }

  /**
   * @return the path in S3 to upload the file to
   */
  protected String getUploadPath() {
    return mKey;
  }
}
