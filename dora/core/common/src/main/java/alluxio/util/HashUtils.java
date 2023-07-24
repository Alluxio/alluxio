package alluxio.util;

import static com.google.common.hash.Hashing.murmur3_32_fixed;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.hash.HashFunction;
import org.apache.commons.codec.binary.Hex;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Util class for hashing
 */
public class HashUtils {

  private static final HashFunction HASH_FUNCTION = murmur3_32_fixed();

  /**
   * Hash the given obj as string.
   * @param object
   * @return hash in string
   */
  public static String hashAsStr(String object) {
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
   * Hash the give obj as long.
   * @param object
   * @return hash in long
   */
  public static long hashAsLong(String object) {
    return HASH_FUNCTION.hashString(object, UTF_8).padToLong();
  }
}
