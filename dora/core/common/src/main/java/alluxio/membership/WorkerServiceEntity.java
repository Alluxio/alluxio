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

package alluxio.membership;

import alluxio.annotation.SuppressFBWarnings;
import alluxio.grpc.GrpcUtils;
import alluxio.util.HashUtils;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Entity class including all the information to register to Etcd
 * when using EtcdMembershipManager.
 */
public class WorkerServiceEntity extends ServiceEntity {
  /**
   * Membership state of the worker.
   */
  enum State {
    JOINED,
    AUTHORIZED,
    DECOMMISSIONED
  }

  WorkerNetAddress mAddress;
  State mState = State.JOINED;
  @SuppressFBWarnings({"URF_UNREAD_FIELD"})
  int mGenerationNum = -1;

  /**
   * CTOR for WorkerServiceEntity.
   */
  public WorkerServiceEntity() {
  }

  /**
   * CTOR  for WorkerServiceEntity with given WorkerNetAddress.
   * @param addr
   */
  public WorkerServiceEntity(WorkerNetAddress addr) {
    super(HashUtils.hashAsStringMD5(addr.dumpMainInfo()));
    mAddress = addr;
    mState = State.AUTHORIZED;
  }

  /**
   * Get WorkerNetAddress field.
   * @return WorkerNetAddress
   */
  public WorkerNetAddress getWorkerNetAddress() {
    return mAddress;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("WorkerId", getServiceEntityName())
        .add("WorkerAddr", mAddress.toString())
        .add("State", mState.toString())
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WorkerServiceEntity anotherO = (WorkerServiceEntity) o;
    return mAddress.equals(anotherO.mAddress)
        && getServiceEntityName().equals(anotherO.getServiceEntityName());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mAddress, mServiceEntityName);
  }

  /**
   * Serialize the WorkerServiceEntity object.
   * @param dos
   * @throws IOException
   */
  public void serialize(DataOutputStream dos) throws IOException {
    super.serialize(dos);
    dos.writeInt(mState.ordinal());
    byte[] serializedArr = GrpcUtils.toProto(mAddress).toByteArray();
    dos.writeInt(serializedArr.length);
    dos.write(serializedArr);
  }

  /**
   * Deserialize to WorkerServiceEntity object.
   * @param dis
   * @throws IOException
   */
  public void deserialize(DataInputStream dis) throws IOException {
    super.deserialize(dis);
    mState = State.values()[dis.readInt()];
    int byteArrLen = dis.readInt();
    byte[] byteArr = new byte[byteArrLen];
    dis.read(byteArr, 0, byteArrLen);
    mAddress = GrpcUtils.fromProto(alluxio.grpc.WorkerNetAddress.parseFrom(byteArr));
  }
}
