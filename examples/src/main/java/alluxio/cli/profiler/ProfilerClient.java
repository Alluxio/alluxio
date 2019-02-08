package alluxio.cli.profiler;

import java.io.IOException;

/**
 * Profiles operations.
 */
public interface ProfilerClient {

  /**
   * Create new.
   */
  class Factory {
    /**
     * Profiler client.
     * @param type alluxio or hadoop
     * @return profiler client
     */
    public static ProfilerClient create(String type) {
      switch (type) {
        case "hadoop":
        case "alluxio":
          return new AlluxioProfilerClient();
        default:
          throw new IllegalArgumentException(String.format("No profiler for %s", type));
      }
    }
  }

  /**
   * Makes sure the path exists without any existing inodes below. Essentially cleaning up any
   * previous operations on the same location.
   *
   * @param dir directory to clean
   * @throws IOException
   */
  void cleanup(String dir) throws IOException;

  /**
   * Makes sure the path exists without any existing inodes below. Essentially cleaning up any
   * previous operations on the same location.
   *
   * @param dir directory to clean
   * @param numFiles the total number of files to create
   * @param filesPerDir number of files to create in each directory
   * @param fileSize the size in bytes of each file that will be created
   * @throws IOException
   */
  void createFiles(String dir, long numFiles, long filesPerDir, long fileSize) throws IOException;
}
