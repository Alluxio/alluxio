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

package alluxio.proxy.s3.signature;

import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * constant key for sign.
 */
public final class SignerConstants {
  public static final String X_AMZ_CREDENTIAL = "X-Amz-Credential";
  public static final String X_AMZ_DATE = "X-Amz-Date";
  public static final String X_AMZ_EXPIRES = "X-Amz-Expires";
  public static final String X_AMZ_SIGNED_HEADER = "X-Amz-SignedHeaders";
  public static final String X_AMZ_CONTENT_SHA256 = "x-Amz-Content-SHA256";
  public static final String X_AMZ_SIGNATURE = "X-Amz-Signature";
  public static final String X_AMZ_ALGORITHM = "X-Amz-Algorithm";
  public static final DateTimeFormatter DATE_FORMATTER
          = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);
  public static final DateTimeFormatter TIME_FORMATTER
          = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);
}
