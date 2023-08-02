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

package alluxio.util;

import static com.google.common.hash.Hashing.murmur3_32_fixed;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.hash.HashFunction;
import org.apache.commons.codec.binary.Hex;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Util class for hashing.
 */
@ThreadSafe
public class HashUtils {

  private static final HashFunction HASH_FUNCTION = murmur3_32_fixed();

  private HashUtils() {} // prevent instantiation

  /**
   * MD5 Hash the given obj as string.
   * @param object
   * @return hash in string
   */
  public static String hashAsStringMD5(String object) {
    try {
      MessageDigest md = MessageDigest.getInstance("MD5");
      md.update(object.getBytes());
      return Hex.encodeHexString(md.digest()).toLowerCase();
    } catch (NoSuchAlgorithmException e) {
      /* No actions. Continue with other hash method. */
    }
    return HASH_FUNCTION.hashString(object, UTF_8).toString();
  }

  /**
   * Hash the give obj as long with given HASH_FUNCTION.
   * @param object
   * @return hash in long
   */
  public static long hashAsLong(String object) {
    return HASH_FUNCTION.hashString(object, UTF_8).padToLong();
  }
}
