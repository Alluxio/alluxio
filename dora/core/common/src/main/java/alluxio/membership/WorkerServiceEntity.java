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
import alluxio.wire.WorkerIdentity;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.InstanceCreator;
import com.google.gson.annotations.Expose;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;

/**
 * Entity class including all the information to register to Etcd
 * when using EtcdMembershipManager.
 */
public class WorkerServiceEntity extends DefaultServiceEntity {
  /**
   * Membership state of the worker.
   */
  enum State {
    JOINED,
    AUTHORIZED,
    DECOMMISSIONED
  }

  @Expose
  @com.google.gson.annotations.SerializedName("Identity")
  WorkerIdentity mIdentity;
  @Expose
  @com.google.gson.annotations.SerializedName("WorkerNetAddress")
  WorkerNetAddress mAddress;
  @Expose
  @com.google.gson.annotations.SerializedName("State")
  State mState = State.JOINED;
  @SuppressFBWarnings({"URF_UNREAD_FIELD"})
  @Expose
  @com.google.gson.annotations.SerializedName("GenerationNumber")
  int mGenerationNum = -1;

  /**
   * CTOR for WorkerServiceEntity.
   */
  public WorkerServiceEntity() {}

  /**
   * CTOR  for WorkerServiceEntity with given WorkerNetAddress.
   *
   * @param identity
   * @param addr
   */
  public WorkerServiceEntity(WorkerIdentity identity, WorkerNetAddress addr) {
    super(identity.toString());
    mIdentity = identity;
    mAddress = addr;
    mState = State.AUTHORIZED;
  }

  /**
   * Get WorkerNetAddress field.
   *
   * @return WorkerNetAddress
   */
  public WorkerNetAddress getWorkerNetAddress() {
    return mAddress;
  }

  /**
   * @return worker identity
   */
  public WorkerIdentity getIdentity() {
    return mIdentity;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("WorkerId", mIdentity)
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
    // only the service name and the identity are the factors that define
    // what a WorkerServiceEntity is. Any other is either ephemeral or supplementary data.
    return mIdentity.equals(anotherO.mIdentity)
        && getServiceEntityName().equals(anotherO.getServiceEntityName());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mIdentity, mServiceEntityName);
  }

  @Override
  public void deserialize(byte[] buf) {
    WorkerServiceEntity obj = this;
    InstanceCreator<DefaultServiceEntity> creator = type -> obj;
    Gson gson = new GsonBuilder()
        .excludeFieldsWithoutExposeAnnotation()
        .registerTypeAdapter(WorkerServiceEntity.class, creator)
        .create();
    gson.fromJson(new InputStreamReader(new ByteArrayInputStream(buf)), WorkerServiceEntity.class);
  }
}
