/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.master;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * Utils for serializating and deserializing information to and from zookeeper node names.
 */
public final class MasterZookeeperNode {
  private final String mHostname;
  private final int mRpcPort;
  private final int mWebPort;

  /**
   * @param hostname the hostname for the master
   * @param rpcPort the rpc port
   * @param webPort the web port
   */
  public MasterZookeeperNode(String hostname, int rpcPort, int webPort) {
    mHostname = hostname;
    mRpcPort = rpcPort;
    mWebPort = webPort;
  }

  /**
   * @return the serialized name for this master zookeeper node
   */
  public String serialize() {
    return String.format("%s:%d:%d", mHostname, mRpcPort, mWebPort);
  }

  /**
   * Deserializes a master zookeeper node it's string representation.
   *
   * @param name the string representation
   */
  public static MasterZookeeperNode deserialize(String name) {
    String[] parts = name.split(":");
    Preconditions.checkState(parts.length == 3, "Master zookeeper nodes must be in the form "
        + "name:rpcPort:webPort, but the specified node has name '%s'", name);
    int rpcPort = Integer.parseInt(parts[1]);
    int webPort = Integer.parseInt(parts[2]);
    return new MasterZookeeperNode(parts[0], rpcPort, webPort);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MasterZookeeperNode)) {
      return false;
    }
    MasterZookeeperNode that = (MasterZookeeperNode) o;
    return Objects.equal(mHostname, that.mHostname) &&
        Objects.equal(mRpcPort, that.mRpcPort) &&
        Objects.equal(mWebPort, that.mWebPort);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mHostname, mRpcPort, mWebPort);
  }
}
