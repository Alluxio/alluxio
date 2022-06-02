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

package alluxio.master.file.cmdmanager.command;

/**
 * Class for bandwidth.
 */
public class Bandwidth {
  private long mBandWidth;

  /**
   * Constructor.
   * @param bandWidth bandWidth value
   */
  public Bandwidth(long bandWidth) {
    mBandWidth = bandWidth;
  }

  /**
   * Get bandwidth.
   * @return bandwidth value
   */
  public long getBandWidthValue() {
    return mBandWidth;
  }

  /**
   * Update bandwidth with new value.
   * @param bandWidth new bandwidth value
   */
  public void update(long bandWidth) {
    mBandWidth = bandWidth;
  }
}
