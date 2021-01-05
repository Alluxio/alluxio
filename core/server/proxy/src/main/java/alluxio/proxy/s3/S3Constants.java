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

package alluxio.proxy.s3;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Constants for S3 northbound API.
 */
@ThreadSafe
public final class S3Constants {
  public static final String S3_CONTENT_LENGTH_HEADER = "Content-Length";
  public static final String S3_ETAG_HEADER = "ETAG";
  public static final String S3_DATE_FORMAT_REGEXP = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
  public static final String S3_STANDARD_STORAGE_CLASS = "STANDARD";

  private S3Constants() {} // prevent instantiation
}
