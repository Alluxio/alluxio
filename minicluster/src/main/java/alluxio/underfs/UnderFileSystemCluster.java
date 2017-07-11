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

import com.google.common.base.Preconditions;
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
    LOG.info("Using default {} for integration testing.", DefaultUnderFileSystemCluster.class.getName());
    return new DefaultUnderFileSystemCluster(baseDir);
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
   */
  public abstract void cleanup() throws IOException;

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
   */
  public void registerJVMOnExistHook() throws IOException {
    Runtime.getRuntime().addShutdownHook(new ShutdownHook(this));
  }

  /**
   * Stops the underfs cluster system.
   */
  public abstract void shutdown() throws IOException;

  /**
   * Starts the underfs cluster system.
   */
  public abstract void start() throws IOException;
}
