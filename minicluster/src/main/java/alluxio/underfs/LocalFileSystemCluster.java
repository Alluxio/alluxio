/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.underfs;

import alluxio.Configuration;

import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The mock cluster for local file system.
 */
@NotThreadSafe
public class LocalFileSystemCluster extends UnderFileSystemCluster {

  /**
   * @param baseDir the base directory
   * @param configuration the configuration for Alluxio
   */
  public LocalFileSystemCluster(String baseDir, Configuration configuration) {
    super(baseDir, configuration);
  }

  @Override
  public String getUnderFilesystemAddress() {
    return new File(mBaseDir).getAbsolutePath();
  }

  @Override
  public boolean isStarted() {
    return true;
  }

  @Override
  public void shutdown() throws IOException {}

  @Override
  public void start() throws IOException {}
}
