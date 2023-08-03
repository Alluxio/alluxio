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

package alluxio.master.scheduler;

import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.membership.MembershipManager;
import alluxio.resource.CloseableResource;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import java.io.IOException;
import java.util.List;

/**
 * MembershipManager backed WorkerProvider for Scheduler.
 */
public class MembershipManagerWorkerProvider implements WorkerProvider {
  private final MembershipManager mMembershipManager;
  private final FileSystemContext mContext;

  /**
   * CTOR for MembershipManagerWorkerProvider.
   * @param membershipMgr
   * @param context
   */
  public MembershipManagerWorkerProvider(MembershipManager membershipMgr,
                                         FileSystemContext context) {
    mMembershipManager = membershipMgr;
    mContext = context;
  }

  @Override
  public List<WorkerInfo> getWorkerInfos() {
    try {
      return mMembershipManager.getAllMembers();
    } catch (IOException ex) {
      throw AlluxioRuntimeException.from(ex);
    }
  }

  @Override
  public List<WorkerInfo> getLiveWorkerInfos() {
    try {
      return mMembershipManager.getLiveMembers();
    } catch (IOException ex) {
      throw AlluxioRuntimeException.from(ex);
    }
  }

  @Override
  public CloseableResource<BlockWorkerClient> getWorkerClient(WorkerNetAddress address) {
    try {
      return mContext.acquireBlockWorkerClient(address);
    } catch (IOException e) {
      throw AlluxioRuntimeException.from(e);
    }
  }
}
