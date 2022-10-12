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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Constants for S3 northbound API.
 */
@ThreadSafe
public final class S3Constants {
  /**
   * Bucket must be a directory directly under a mount point. If it is under a non-root mount point,
   * the bucket separator must be used as the separator in the bucket name, for example,
   * mount:point:bucket represents Alluxio directory /mount/point/bucket.
   */
  public static final String BUCKET_SEPARATOR = ":";

  /* Headers */
  // standard headers
  public static final String S3_CONTENT_TYPE_HEADER = "Content-Type";
  public static final String S3_CONTENT_LENGTH_HEADER = "Content-Length";

  // AWS headers
  public static final String S3_ACL_HEADER = "x-amz-acl";
  public static final String S3_COPY_SOURCE_HEADER = "x-amz-copy-source";
  public static final String S3_ETAG_HEADER = "ETAG";
  public static final String S3_METADATA_DIRECTIVE_HEADER = "x-amz-metadata-directive";

  public static final String S3_TAGGING_HEADER = "x-amz-tagging";
  public static final String S3_TAGGING_COUNT_HEADER = "x-amz-tagging-count";
  public static final String S3_TAGGING_DIRECTIVE_HEADER = "x-amz-tagging-directive";

  public static final String S3_SIGN_CREDENTIAL = "X-Amz-Credential";
  public static final String S3_SIGN_DATE = "X-Amz-Date";
  public static final String S3_SIGN_EXPIRES = "X-Amz-Expires";
  public static final String S3_SIGN_SIGNED_HEADER = "X-Amz-SignedHeaders";
  public static final String S3_SIGN_SIGNATURE = "X-Amz-Signature";
  public static final String S3_SIGN_ALGORITHM = "X-Amz-Algorithm";
  public static final String S3_SIGN_CONTENT_SHA256 = "X-Amz-Content-SHA256";

  /* xAttr keys */
  public static final String CONTENT_TYPE_XATTR_KEY = "s3_content_type";
  public static final String ETAG_XATTR_KEY = "s3_etag";
  public static final String TAGGING_XATTR_KEY = "s3_tags";
  public static final String UPLOADS_BUCKET_XATTR_KEY = "s3_uploads_bucket";
  public static final String UPLOADS_OBJECT_XATTR_KEY = "s3_uploads_object";
  public static final String UPLOADS_FILE_ID_XATTR_KEY = "s3_uploads_file_id";

  /* Alluxio UFS metadata */
  public static final String S3_METADATA_ROOT_DIR = ".alluxio_s3_api_metadata";
  public static final String S3_METADATA_UPLOADS_DIR = "uploads";

  /* Charsets */
  public static final Charset AUTHORIZATION_CHARSET = StandardCharsets.UTF_8;
  public static final Charset HEADER_CHARSET = StandardCharsets.UTF_8;
  public static final Charset TAGGING_CHARSET = StandardCharsets.UTF_8;
  public static final Charset XATTR_STR_CHARSET = StandardCharsets.UTF_8;

  /* Misc. */
  public static final DateTimeFormatter DATE_FORMATTER
          = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);
  public static final DateTimeFormatter TIME_FORMATTER
          = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);

  public static final String S3_DATE_FORMAT_REGEXP = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
  public static final String S3_STANDARD_STORAGE_CLASS = "STANDARD";

  // TODO(czhu): prefix multipart upload part file names with this
  public static final String S3_MULTIPART_PART_PREFIX = "part_";

  /**
   * Directive specifies whether metadata/tag-set are copied from the source object
   * or replaced with metadata/tag-set provided in the request.
   */
  public enum Directive {
    COPY,
    REPLACE
  }

  private S3Constants() {} // prevent instantiation
}
