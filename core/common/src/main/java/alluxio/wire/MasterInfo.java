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

package alluxio.wire;

import com.google.common.base.Objects;

import java.io.Serializable;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Information about the Alluxio master.
 */
@NotThreadSafe
public final class MasterInfo implements Serializable {
  private static final long serialVersionUID = 5846173765139223974L;

  private int mWebPort;

  /**
   * Creates a {@link MasterInfo} with all fields set to default values.
   */
  public MasterInfo() {}

  /**
   * @return the master web port
   */
  public int getWebPort() {
    return mWebPort;
  }

  /**
   * @param webPort the web port to set
   * @return the updated master info object
   */
  public MasterInfo setWebPort(int webPort) {
    mWebPort = webPort;
    return this;
  }

  /**
   * @return thrift representation of the master information
   */
  public alluxio.thrift.MasterInfo toThrift() {
    return new alluxio.thrift.MasterInfo().setWebPort(mWebPort);
  }

  /**
   * @param info the thrift master info to create a wire master info from
   * @return the wire type version of the master info
   */
  public static MasterInfo fromThrift(alluxio.thrift.MasterInfo info) {
    return new MasterInfo().setWebPort(info.getWebPort());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MasterInfo)) {
      return false;
    }
    MasterInfo that = (MasterInfo) o;
    return Objects.equal(mWebPort, that.mWebPort);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mWebPort);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("webPort", mWebPort)
        .toString();
  }

  /**
   * Enum representing the fields of the master info.
   */
  public static enum MasterInfoField {
    WEB_PORT;

    /**
     * @return the thrift representation of this master info field
     */
    public alluxio.thrift.MasterInfoField toThrift() {
      return alluxio.thrift.MasterInfoField.valueOf(name());
    }

    /**
     * @param field the thrift representation of the master info field to create
     * @return the wire type version of the master info field
     */
    public static MasterInfoField fromThrift(alluxio.thrift.MasterInfoField field) {
      return MasterInfoField.valueOf(field.name());
    }
  }
}
