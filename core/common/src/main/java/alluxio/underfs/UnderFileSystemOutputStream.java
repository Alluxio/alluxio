package alluxio.underfs;

import java.util.Optional;

/**
 * Interface for returning the content hash. Instances of {@link java.io.OutputStream} returned by
 * {@link UnderFileSystem#create} may implement this interface if the UFS returns the has of the
 * content written after the stream is closed.
 */
public interface UnderFileSystemOutputStream {
  /**
   * @return the content hash of the file written to the UFS if available
   */
  Optional<String> getContentHash();
}
