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

package alluxio.underfs;

import alluxio.AlluxioURI;
import alluxio.Constants;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility functions for testing with {@link UnderFileSystem}.
 *
 */
@ThreadSafe
public final class UnderFileSystemTestUtils {
  /**
   * @param ufs the {@link UnderFileSystem} implementation to check
   * @return true if the implementation is a Google cloud store implementation
   */
  public static boolean isGcs(UnderFileSystem ufs) {
    return "gcs".equals(ufs.getUnderFSType());
  }

  /**
   * @param ufs the {@link UnderFileSystem} implementation to check
   * @return true if the implementation is a local file system implementation
   */
  public static boolean isLocal(UnderFileSystem ufs) {
    return "local".equals(ufs.getUnderFSType());
  }

  /**
   * @param ufs the {@link UnderFileSystem} implementation to check
   * @return true if the implementation is a Hadoop distributed file system implementation
   */
  public static boolean isHdfs(UnderFileSystem ufs) {
    return "hdfs".equals(ufs.getUnderFSType());
  }

  /**
   * Returns whether the given ufs address indicates a object storage ufs.
   *
   * @param ufsAddress the ufs address
   * @return true if the under file system is a object storage; false otherwise
   */
  public static boolean isObjectStorage(String ufsAddress) {
    return ufsAddress.startsWith(Constants.HEADER_S3)
        || ufsAddress.startsWith(Constants.HEADER_S3A)
        || ufsAddress.startsWith(Constants.HEADER_GCS)
        || ufsAddress.startsWith(Constants.HEADER_SWIFT)
        || ufsAddress.startsWith(Constants.HEADER_OSS);
  }

  /**
   * @param ufs the {@link UnderFileSystem} implementation to check
   * @return true if the implementation is an object storage implementation
   */
  public static boolean isObjectStorage(UnderFileSystem ufs) {
    return ufs.isObjectStorage();
  }

  /**
   * @param ufs the {@link UnderFileSystem} implementation to check
   * @return true if the implementation is an Object storage service implementation
   */
  public static boolean isOss(UnderFileSystem ufs) {
    return "oss".equals(ufs.getUnderFSType());
  }

  /**
   * @param ufs the {@link UnderFileSystem} implementation to check
   * @return true if the implementation is an S3 implementation
   */
  public static boolean isS3(UnderFileSystem ufs) {
    return "s3".equals(ufs.getUnderFSType()) || "s3a".equals(ufs.getUnderFSType());
  }

  /**
   * @param ufs the {@link UnderFileSystem} implementation to check
   * @return true if the implementation is a Swift storage implementation
   */
  public static boolean isSwift(UnderFileSystem ufs) {
    return "swift".equals(ufs.getUnderFSType());
  }

  /**
   * @param uri the UFS path
   * @return the bucket or container name of the object storage
   */
  public static String getBucketName(AlluxioURI uri) {
    return uri.getAuthority();
  }

  private UnderFileSystemTestUtils() {} // prevent instantiation
}
