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

import alluxio.proxy.s3.S3Exception;

/**
 * Parser to request auth parser for http request.
 */
public interface SignatureProcessor {

  /**
   * API to return string to sign.
   * @return a string to sign
   */
  SignatureInfo parseSignature() throws S3Exception;

  /**
   * Convert SignatureInfo to SignedInfo.
   * @return SignedInfo
   * @throws S3Exception
   */
  SignedInfo getSignedInfo() throws S3Exception;
}
