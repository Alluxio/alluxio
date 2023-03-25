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

package alluxio.client;

import org.apache.commons.codec.binary.Hex;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Represents an unique identifier of a path.
 */
public interface PathIdentifier {
  /**
   * @return the path id
   */
  String getId();

  /**
   * @return the full ufs path
   */
  String getUfsPath();

  /**
   * @return the version of the path
   */
  default String getVersion() {
    throw new UnsupportedOperationException();
  }

  /**
   * Computes the hash of the given path, with SHA-256, or MD5, or simple hashCode().
   *
   * @param path the path to hash
   * @return HEX encoded hash string
   */
  static String hash(String path) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      md.update(path.getBytes());
      return Hex.encodeHexString(md.digest()).toLowerCase();
    } catch (NoSuchAlgorithmException e) {
      /* No actions. Continue with other hash method. */
    }

    try {
      MessageDigest md = MessageDigest.getInstance("MD5");
      md.update(path.getBytes());
      return Hex.encodeHexString(md.digest()).toLowerCase();
    } catch (NoSuchAlgorithmException e) {
      /* No actions. Continue with other hash method. */
    }

    // Cannot find SHA-256 or MD5. Fall back to use simple hashCode, which is probable to conflict.
    return Hex.encodeHexString(String.valueOf(path.hashCode()).getBytes()).toLowerCase();
  }
}
