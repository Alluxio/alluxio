package tachyon.underfs;

import tachyon.conf.TachyonConf;

/**
 * Interface for under file system factories
 *
 */
public interface UnderFileSystemFactory {

  /**
   * Gets whether this factory supports the given path and thus whether calling the
   * {@link #create(String, Object)} can succeed for this path
   * 
   * @param path
   *          File path
   * @param tachyonConf
   *          Tachyon configuration
   * @return True if the path is supported, false otherwise
   */
  public boolean supportsPath(String path, TachyonConf tachyonConf);

  /**
   * Creates a client for accessing the given path
   * 
   * @param path
   *          File path
   * @param tachyonConf
   *          Tachyon configuration
   * @param conf
   *          Optional configuration object for the UFS, may be null
   * @return Client
   * @throws IllegalArgumentException
   *           Thrown if this factory does not support clients for the given path or if the
   *           configuration provided is insufficient to create a client
   */
  public UnderFileSystem create(String path, TachyonConf tachyonConf, Object conf);
}
