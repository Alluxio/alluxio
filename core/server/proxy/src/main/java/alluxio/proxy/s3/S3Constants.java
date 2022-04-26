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
import javax.annotation.concurrent.ThreadSafe;

/**
 * Constants for S3 northbound API.
 */
@ThreadSafe
public final class S3Constants {
  /* Headers */
  public static final String S3_CONTENT_LENGTH_HEADER = "Content-Length";

  public static final String S3_ACL_HEADER = "x-amz-acl";
  public static final String S3_COPY_SOURCE_HEADER = "x-amz-copy-source";
  public static final String S3_ETAG_HEADER = "ETAG";
  public static final String S3_METADATA_DIRECTIVE_HEADER = "x-amz-metadata-directive";
  public static final String S3_TAGGING_HEADER = "x-amz-tagging";
  public static final String S3_TAGGING_COUNT_HEADER = "x-amz-tagging-count";
  public static final String S3_TAGGING_DIRECTIVE_HEADER = "x-amz-tagging-directive";

  public static final String S3_DATE_FORMAT_REGEXP = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
  public static final String S3_STANDARD_STORAGE_CLASS = "STANDARD";

  /* Headers xAttr. */
  public static final String CONTENT_TYPE_XATTR_KEY = "s3_content_type";
  public static final Charset HEADER_CHARSET = StandardCharsets.UTF_8;

  /* S3 Metadata tagging. */
  public static final String TAGGING_XATTR_KEY = "s3_tags";
  public static final Charset TAGGING_CHARSET = StandardCharsets.UTF_8;

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
