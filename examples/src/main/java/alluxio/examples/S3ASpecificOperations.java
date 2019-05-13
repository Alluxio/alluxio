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

package alluxio.examples;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.conf.InstancedConfiguration;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.s3a.S3ALowLevelOutputStream;
import alluxio.underfs.s3a.S3AUnderFileSystem;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.io.PathUtils;

import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;

/**
 * Examples for S3A specific under filesystem operations. This class tests
 * S3A specific operations like streaming upload.
 */
public final class S3ASpecificOperations {
  private static final Logger LOG = LoggerFactory.getLogger(S3ASpecificOperations.class);
  private static final byte[] TEST_BYTES = "TestBytesInS3AFiles".getBytes();

  private static final String INIT_OPS = "AmazonS3Client.initiateMultipartUpload()";
  private static final String UPLOAD_PART_OPS = "AmazonS3Client.uploadPart()";
  private static final String COMPLETE_OPS = "AmazonS3Client.completeMultipartUpload()";
  private static final String LIST_UPLOAD_OPS = "AmazonS3Client.listMultipartUploads()";
  private static final String LIST_OBJECT_OPS = "AmazonS3Client.listObjects()";
  private static final String ABORT_OPS = "TransferManager.abortMultipartUploads()";

  private String mBucket;
  private String mUnderfsAddress;
  private S3AUnderFileSystem mUfs;
  private InstancedConfiguration mConfiguration;

  /**
   * @param bucket the S3A bucket
   * @param underfsAddress the directory to run tests against
   * @param ufs the S3A under file system
   * @param configuration the instance configuration
   */
  public S3ASpecificOperations(String bucket, String underfsAddress,
      S3AUnderFileSystem ufs, InstancedConfiguration configuration) {
    mBucket = bucket;
    mUnderfsAddress = underfsAddress;
    mUfs = ufs;
    mConfiguration = configuration;
  }

  /**
   * Runs all the tests.
   */
  public void runTests() throws IOException {
    createEmptyFileUsingStreamingUpload();
    createFileLessThanOnePart();
    createMultipartFile();
    createAndAbortMultipartFile();
  }

  private void createEmptyFileUsingStreamingUpload() throws IOException {
    LOG.info("Running test: create and read empty file using streaming upload");
    String testFile = PathUtils.concatPath(mUnderfsAddress, "createOpenEmpty");
    int bytesRead;
    OutputStream o = mUfs.create(testFile);
    o.close();
    byte[] buf = new byte[0];
    bytesRead = mUfs.open(testFile).read(buf);
    if (bytesRead != 0) {
      throw new IOException("The written file should be empty but is not");
    }
    afterTest();
  }

  private void createFileLessThanOnePart() throws IOException {
    LOG.info("Running test: create and read file less than one part using streaming upload");
    String testFile = PathUtils.concatPath(mUnderfsAddress, "createOpen");
    byte[] buf;
    int bytesRead;
    try {
      OutputStream o = mUfs.create(testFile);
      o.write(TEST_BYTES);
      o.close();
      buf = new byte[TEST_BYTES.length];
      bytesRead = mUfs.open(testFile).read(buf);
    } catch (Throwable t) {
      logS3AOperationInfo(Arrays.asList(INIT_OPS, UPLOAD_PART_OPS, COMPLETE_OPS));
      throw t;
    }
    if (TEST_BYTES.length != bytesRead || !Arrays.equals(buf, TEST_BYTES)) {
      throw new IOException("The written content is incorrect");
    }
    afterTest();
  }

  private void createMultipartFile() throws IOException {
    LOG.info("Running test: create and read multipart file using streaming upload");
    String testParent = PathUtils.concatPath(mUnderfsAddress, "createParent");
    String testFile = PathUtils.concatPath(testParent, "createOpenLarge");
    int numCopies;
    try (OutputStream outputStream = mUfs.create(testFile, CreateOptions.defaults(mConfiguration)
        .setCreateParent(true))) {
      numCopies = 6 * Constants.MB / TEST_BYTES.length;
      for (int i = 0; i < numCopies; ++i) {
        outputStream.write(TEST_BYTES);
      }
    } catch (Throwable t) {
      logS3AOperationInfo(Arrays.asList(INIT_OPS, UPLOAD_PART_OPS, COMPLETE_OPS));
      throw t;
    }
    // Validate data written successfully and content is correct
    try (InputStream inputStream = mUfs.openExistingFile(testFile)) {
      byte[] buf = new byte[numCopies * TEST_BYTES.length];
      int offset = 0;
      while (offset < buf.length) {
        int bytesRead = inputStream.read(buf, offset, buf.length - offset);
        for (int i = 0; i < bytesRead; ++i) {
          if (TEST_BYTES[(offset + i) % TEST_BYTES.length] != buf[offset + i]) {
            throw new IOException("The written content is incorrect");
          }
        }
        offset += bytesRead;
      }
    }

    try {
      // These two calls will trigger S3A list objects v1
      if (!mUfs.isDirectory(testParent)) {
        throw new IOException("The written directory does not exist or is file");
      }
      UfsStatus[] statuses = mUfs.listStatus(PathUtils.concatPath(mUnderfsAddress, "createParent"));
      if (statuses.length != 1) {
        throw new IOException("Failed to list status in the written directory");
      }
    } catch (Throwable t) {
      logS3AOperationInfo(Arrays.asList(LIST_OBJECT_OPS));
      throw t;
    }
    afterTest();
  }

  private void createAndAbortMultipartFile() throws IOException {
    LOG.info("Running test: create and abort multipart file");
    String testFile = PathUtils.concatPath(mUnderfsAddress, "createAndAbort");
    try {
      OutputStream outputStream = mUfs.create(testFile);
      // Clean all intermediate multipart uploads
      mUfs.cleanup();
      if (getMultipartUploadList().size() != 0) {
        throw new IOException("Failed to clean previous intermediate multipart uploads.");
      }

      // Create a file but do not close it
      int numCopies = 6 * Constants.MB / TEST_BYTES.length;
      for (int i = 0; i < numCopies; ++i) {
        outputStream.write(TEST_BYTES);
      }
      // Multipart upload that has in progress upload will not be aborted
      ((S3ALowLevelOutputStream) outputStream).waitForAllPartsUpload();
      int size = getMultipartUploadList().size();
      if (size != 1) {
        throw new IOException(String.format("Expected to have one intermediate multipart upload, "
            + "but get %s in %s", size, mBucket));
      }
      mUfs.cleanup();
      if (getMultipartUploadList().size() != 0) {
        throw new IOException("Failed to clean the in progress multipart upload");
      }
    } catch (Throwable t) {
      logS3AOperationInfo(Arrays.asList(INIT_OPS, UPLOAD_PART_OPS, LIST_UPLOAD_OPS, ABORT_OPS));
      throw t;
    }
    afterTest();
  }

  private void afterTest() throws IOException {
    UfsStatus[] statuses = mUfs.listStatus(mUnderfsAddress);
    for (UfsStatus status : statuses) {
      if (status instanceof UfsFileStatus) {
        mUfs.deleteFile(PathUtils.concatPath(mUnderfsAddress, status.getName()));
      } else {
        mUfs.deleteDirectory(PathUtils.concatPath(mUnderfsAddress, status.getName()),
            DeleteOptions.defaults().setRecursive(true));
      }
    }
    LOG.info("Test passed");
  }

  /**
   * @return a list of intermediate multipart uploads
   */
  private List<MultipartUpload> getMultipartUploadList() {
    return mUfs.getS3Client().listMultipartUploads(
        new ListMultipartUploadsRequest(UnderFileSystemUtils
            .getBucketName(new AlluxioURI(mBucket)))).getMultipartUploads();
  }

  private void logS3AOperationInfo(List<String> operations) {
    LOG.error("This test includes {} S3A operations.",
        String.join(", ", operations));
  }
}
