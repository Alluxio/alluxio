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
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.exception.runtime.UnavailableRuntimeException;
import alluxio.exception.status.UnavailableException;
import alluxio.membership.MembershipManager;
import alluxio.resource.CloseableResource;
import alluxio.scheduler.job.WorkerProvider;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class MembershipManagerWorkerProvider implements WorkerProvider {
  private final MembershipManager mMembershipManager;
  private final FileSystemContext mContext;

  public MembershipManagerWorkerProvider(MembershipManager membershipMgr, FileSystemContext context) {
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
