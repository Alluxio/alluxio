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

package alluxio.underfs;

import alluxio.AlluxioURI;
import alluxio.underfs.options.DeleteOptions;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Base class for a UFS cluster.
 */
@NotThreadSafe
public abstract class UnderFileSystemCluster {
  private static final Logger LOG = LoggerFactory.getLogger(UnderFileSystemCluster.class);

  class ShutdownHook extends Thread {

    UnderFileSystemCluster mUFSCluster = null;

    public ShutdownHook(UnderFileSystemCluster ufsCluster) {
      mUFSCluster = ufsCluster;
    }

    @Override
    public void run() {
      if (mUFSCluster != null) {
        try {
          mUFSCluster.shutdown();
        } catch (IOException e) {
          LOG.warn("Failed to shutdown underfs cluster: {}" + e.getMessage());
        }
      }
    }
  }

  private static final String INTEGRATION_UFS_PROFILE_KEY = "ufs";

  private static String sUnderFSClass;

  private static UnderFileSystemCluster sUnderFSCluster = null;

  /**
   * @return the existing underfs, throwing IllegalStateException if it hasn't been initialized yet
   */
  public static synchronized UnderFileSystemCluster get() {
    Preconditions.checkNotNull(sUnderFSCluster, "sUnderFSCluster has not been initialized yet");
    return sUnderFSCluster;
  }

  /**
   * Creates an underfs test bed and register the shutdown hook.
   *
   * @param baseDir base directory
   * @throws IOException when the operation fails
   * @return an instance of the UnderFileSystemCluster class
   */
  public static synchronized UnderFileSystemCluster get(String baseDir)
      throws IOException {
    if (sUnderFSCluster == null) {
      sUnderFSCluster = getUnderFilesystemCluster(baseDir);
    }

    if (!sUnderFSCluster.isStarted()) {
      sUnderFSCluster.start();
      sUnderFSCluster.registerJVMOnExistHook();
    }

    return sUnderFSCluster;
  }

  /**
   * Gets the {@link UnderFileSystemCluster}.
   *
   * @param baseDir the base directory
   * @return the {@link UnderFileSystemCluster}
   */
  public static UnderFileSystemCluster getUnderFilesystemCluster(String baseDir) {
    sUnderFSClass = System.getProperty(INTEGRATION_UFS_PROFILE_KEY);

    if (!StringUtils.isEmpty(sUnderFSClass)) {
      try {
        UnderFileSystemCluster ufsCluster =
            (UnderFileSystemCluster) Class.forName(sUnderFSClass).getConstructor(String.class)
                .newInstance(baseDir);
        LOG.info("Initialized ufs cluster {} for integration testing.", sUnderFSClass);
        return ufsCluster;
      } catch (Exception e) {
        LOG.warn("Failed to initialize the ufs cluster {} for integration testing: {}",
            sUnderFSClass, e.getMessage());
        throw Throwables.propagate(e);
      }
    }
    LOG.info("Using default {} for integration testing.", LocalFileSystemCluster.class.getName());
    return new LocalFileSystemCluster(baseDir);
  }

  /**
   *
   * @return the {@link UnderFileSystem} class name
   */
  public static synchronized String getUnderFSClass() {
    return sUnderFSClass;
  }

  protected String mBaseDir;

  /**
   * @param baseDir the base directory
   */
  public UnderFileSystemCluster(String baseDir) {
    mBaseDir = baseDir;
  }

  /**
   * Cleans up the test environment over underfs cluster system, so that we can re-use the running
   * system for the next test round instead of turning on/off it from time to time. This function is
   * expected to be called either before or after each test case which avoids certain overhead from
   * the bootstrap.
   *
   * @throws IOException when the operation fails
   */
  public void cleanup() throws IOException {
    if (isStarted()) {
      String path = getUnderFilesystemAddress() + AlluxioURI.SEPARATOR;
      UnderFileSystem ufs = UnderFileSystem.Factory.get(path);
      for (UnderFileStatus p : ufs.listStatus(path)) {
        String childPath = PathUtils.concatPath(path, p.getName());
        if (p.isDirectory()) {
          ufs.deleteDirectory(childPath, DeleteOptions.defaults().setRecursive(true));
        } else {
          ufs.deleteFile(childPath);
        }
      }
    }
  }

  /**
   * @return the address of the UFS
   */
  public abstract String getUnderFilesystemAddress();

  /**
   * @return if the cluster has started
   */
  public abstract boolean isStarted();

  /**
   * Adds a shutdown hook. The {@link #shutdown()} phase will be automatically called while the
   * process exists.
   *
   * @throws IOException when the operation fails
   */
  public void registerJVMOnExistHook() throws IOException {
    Runtime.getRuntime().addShutdownHook(new ShutdownHook(this));
  }

  /**
   * Stops the underfs cluster system.
   *
   * @throws IOException when the operation fails
   */
  public abstract void shutdown() throws IOException;

  /**
   * Starts the underfs cluster system.
   *
   * @throws IOException when the operation fails
   */
  public abstract void start() throws IOException;
}
