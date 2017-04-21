package alluxio.worker;

import alluxio.underfs.UnderFileSystem;

import java.io.Closeable;
import java.io.IOException;

/**
 * A class manages the ufs used by different worker services.
 */
public interface UfsManager extends Closeable {
  /**
   * Gets the properties for the given the ufs id. If this ufs id is new to this worker, this method
   * will query master to get the corresponding ufs info.
   *
   * @param id ufs id
   * @return the configuration of the UFS
   * @throws IOException if the file persistence fails
   */
  UnderFileSystem getUfsById(long id) throws IOException;
}
