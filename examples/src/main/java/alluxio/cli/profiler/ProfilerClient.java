package alluxio.cli.profiler;

import alluxio.Constants;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Profiles operations.
 */
public abstract class ProfilerClient {

  protected static final int CHUNK_SIZE = 10 * Constants.MB;
  protected static final byte[] DATA = new byte[CHUNK_SIZE];
  public static boolean sDryRun = false;

  /**
   * Create new.
   */
  public static class Factory {

    private static final AlluxioConfiguration CONF =
        new InstancedConfiguration(ConfigurationUtils.defaults());

    /**
     * Profiler client.
     * @param type alluxio or hadoop
     * @return profiler client
     */
    public static ProfilerClient create(String type) {
      switch (type) {
        case "hadoop":
          return new HadoopProfilerClient(CONF.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS));
        case "alluxio":
          return new AlluxioProfilerClient();
        default:
          throw new IllegalArgumentException(String.format("No profiler for %s", type));
      }
    }
  }

  protected void clientOperation(Runnable action, String msg) {
    if(!sDryRun) {
      action.run();
    } else {
      System.out.println(msg);
    }
  }

  static void writeOutput(OutputStream os, long fileSize) throws IOException {
    for (int k = 0; k < fileSize / CHUNK_SIZE; k++) {
      os.write(DATA);
    }
    os.write(DATA, 0, (int)(fileSize % CHUNK_SIZE)); // Write any remaining data
  }

  /**
   * Makes sure the path exists without any existing inodes below. Essentially cleaning up any
   * previous operations on the same location.
   *
   * @param dir directory to clean
   * @throws IOException
   */
  public abstract void cleanup(String dir) throws IOException;

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
  public abstract void createFiles(String dir, long numFiles, long filesPerDir, long fileSize) throws IOException;
}
