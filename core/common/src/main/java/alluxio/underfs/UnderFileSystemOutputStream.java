package alluxio.underfs;

import java.util.Optional;

/**
 * Interface for returning the content hash. Instances of {@link java.io.OutputStream} returned by
 * {@link UnderFileSystem#create} may implement this interface if the UFS returns the hash of the
 * content written when the stream is closed. The content hash will then be used as part of
 * the metadata fingerprint when the file is completed on the Alluxio master.
 */
public interface UnderFileSystemOutputStream {
  /**
   * @return the content hash of the file written to the UFS if available
   * after the stream has been closed
   */
  Optional<String> getContentHash();
}
