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

import alluxio.wire.WorkerInfo;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * No-op membership manager to disable MembershipManager module
 * as default for regression purpose.
 */
public class NoOpMembershipManager implements MembershipManager {

  /**
   * @return NoOpMembershipManager
   */
  public static NoOpMembershipManager create() {
    return new NoOpMembershipManager();
  }

  @Override
  public void join(WorkerInfo worker) throws IOException {
    // NO-OP
  }

  @Override
  public List<WorkerInfo> getAllMembers() throws IOException {
    return Collections.emptyList();
  }

  @Override
  public List<WorkerInfo> getLiveMembers() throws IOException {
    return Collections.emptyList();
  }

  @Override
  public List<WorkerInfo> getFailedMembers() throws IOException {
    return Collections.emptyList();
  }

  @Override
  public String showAllMembers() {
    return StringUtils.EMPTY;
  }

  @Override
  public void stopHeartBeat(WorkerInfo worker) throws IOException {
    // NO OP
  }

  @Override
  public void decommission(WorkerInfo worker) throws IOException {
    // NO OP
  }

  @Override
  public void close() throws Exception {
    // NO OP
  }
}
