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

package alluxio.master.file.loadmanager.load;

import com.google.common.base.Objects;

/**
 * Class for Load information.
 */
public class LoadInfo {
  private final long mLoadId; // Load Id.
  private final String mPath;
  private final LoadOptions mOptions;

  /**
   * Constructor.
   * @param id load id
   * @param path file path
   * @param bandwidth bandwidth
   */
  public LoadInfo(long id, String path, int bandwidth) {
    mLoadId = id;
    mPath = path;
    mOptions = new LoadOptions(bandwidth);
  }

  /**
   * Get load Id.
   * @return the id
   */
  public long getId() {
    return mLoadId;
  }

  /**
   * Get load file path.
   * @return file path
   */
  public String getPath() {
    return mPath;
  }

  /**
   * Get load options.
   * @return load options
   */
  public LoadOptions getLoadOptions() {
    return mOptions;
  }

  /**
   * LoadOptions class.
   */
  public static class LoadOptions {
    private int mBandwidth;

    /**
     * Create LoadOptions object.
     * @param bandwidth bandwidth as param
     */
    public LoadOptions(int bandwidth) {
      mBandwidth = bandwidth;
    }

    /**
     * Get Bandwidth value.
     * @return bandwidth
     */
    public int getBandwidth() {
      return mBandwidth;
    }

    /**
     * Update bandwidth with new value.
     * @param bandWidth new bandwidth value
     */
    public void setBandwidth(int bandWidth) {
      mBandwidth = bandWidth;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      LoadOptions that = (LoadOptions) o;
      return Objects.equal(mBandwidth, that.mBandwidth);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(mBandwidth);
    }
  }
}
