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

import com.google.common.base.Objects;

/**
 * Class for command information.
 */
public class CommandInfo {
  private long mCommandId; //Command Id.
  private CmdType mType;
  private String mPath;
  private CmdOptions mOptions;

  /**
   * Constructor.
   * @param id command id
   * @param path file path
   * @param type command type
   * @param bandwidth bandwidth
   */
  public CommandInfo(long id, String path, CmdType type, long bandwidth) {
    mCommandId = id;
    mPath = path;
    mType = type;
    mOptions = new CmdOptions(new Bandwidth(bandwidth));
  }

  /**
   * Get command Id.
   * @return the id
   */
  public long getId() {
    return mCommandId;
  }

  /**
   * Get command type.
   * @return command type
   */
  public CmdType getType() {
    return mType;
  }

  /**
   * Get command file path.
   * @return file path
   */
  public String getPath() {
    return mPath;
  }

  /**
   * Get command options.
   * @return command options
   */
  public CmdOptions getCmdOptions() {
    return mOptions;
  }

  /**
   * CmdOptions class.
   */
  public static class CmdOptions {
    private Bandwidth mBandwidth;

    /**
     * Create CmdOptions object.
     * @param bandwidth bandwidth as param
     */
    public CmdOptions(Bandwidth bandwidth) {
      mBandwidth = bandwidth;
    }

    /**
     * Create CmdOptions object.
     * @param bandwidth bandwidth value as param
     */
    public CmdOptions(long bandwidth) {
      this(new Bandwidth(bandwidth));
    }

    /**
     * Get Bandwidth value.
     * @return bandwidth
     */
    public Bandwidth getBandwidth() {
      return mBandwidth;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      CmdOptions that = (CmdOptions) o;
      return Objects.equal(mBandwidth, that.mBandwidth);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(mBandwidth);
    }
  }
}
