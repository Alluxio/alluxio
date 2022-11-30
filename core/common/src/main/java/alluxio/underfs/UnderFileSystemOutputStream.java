package alluxio.underfs;

import java.util.Optional;

/**
 * For returning the content hash.
 */
public interface UnderFileSystemOutputStream {
  /**
   * @return the content hash
   */
  Optional<String> getContentHash();
}
