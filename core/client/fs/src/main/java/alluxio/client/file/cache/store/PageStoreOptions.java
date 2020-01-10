/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 *
 */

package alluxio.client.file.cache.store;

/**
 * Options used to instantiate a {@link alluxio.client.file.cache.PageStore}.
 */
public abstract class PageStoreOptions {

  /**
   * @return the type corresponding to the page store
   */
  public abstract PageStoreType getType();

  /**
   *
   * @param <T> The type corresponding to the underlying options
   * @return the options casted to the required type
   */
  public <T> T toOptions() {
    return (T) this;
  }

  /**
   * Root directory where the data is stored.
   */
  protected String mRootDir;

  /**
   * @param rootDir the root directory where pages are stored
   */
  public void setRootDir(String rootDir) {
    mRootDir = rootDir;
  }

  /**
   * @return the root directory where pages are stored
   */
  public String getRootDir() {
    return mRootDir;
  }
}
