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

import alluxio.grpc.Command;

import com.google.common.base.Objects;

import java.io.Serializable;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * The Alluxio HeartBeat Response Message.
 */
@NotThreadSafe
public final class HeartBeatResponseMessage implements Serializable {

  private static final long serialVersionUID = -8936215337909224255L;
  Command mCommand = null;
  Map<Long, Long> mReplicaInfo = null;

  /**
  * Creates a new instance of {@link HeartBeatResponseMessage}.
  */
  public HeartBeatResponseMessage() {
  }

  /**
  * @return the blocks to be added
  */
  public Command getCommand() {
    return mCommand;
  }

  /**
  * @return the replica info of blocks to be changed for worker
  */
  public Map<Long, Long> getReplicaInfo() {
    return mReplicaInfo;
  }

  /**
   * @param command
   * @return Set the command for heartbeat Response
   */
  public HeartBeatResponseMessage setCommand(Command command) {
    mCommand = command;
    return this;
  }

  /**
   * @param ReplicaInfo
   * @return set the replica Info to be changed
   */
  public HeartBeatResponseMessage setReplicaInfo(Map<Long, Long> ReplicaInfo) {
    mReplicaInfo = ReplicaInfo;
    return this;
  }

  /**
   * @return proto representation of the heartbeat information from master to worker
   */
  protected alluxio.grpc.BlockHeartbeatPResponse toProto() {
    return alluxio.grpc.BlockHeartbeatPResponse.newBuilder().setCommand(mCommand)
            .putAllReplicaInfo(mReplicaInfo).build();
  }

  /**
   * Creates a new instance of {@link HeartBeatResponseMessage} from a proto representation.
   *
   * @param info the proto representation of HeartBeatResponse
   * @return the instance
   */
  public static HeartBeatResponseMessage fromProto(alluxio.grpc.BlockHeartbeatPResponse info) {
    return new HeartBeatResponseMessage().setCommand(info.getCommand())
             .setReplicaInfo(info.getReplicaInfoMap());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof HeartBeatResponseMessage)) {
      return false;
    }
    HeartBeatResponseMessage that = (HeartBeatResponseMessage) o;
    return mCommand == that.mCommand && Objects.equal(mReplicaInfo, that.mReplicaInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mCommand, mReplicaInfo);
  }
}
