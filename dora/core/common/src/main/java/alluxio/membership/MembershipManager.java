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

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.resource.LockResource;
import alluxio.wire.WorkerInfo;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.concurrent.GuardedBy;

/**
 * Interface for worker membership management module.
 */
public interface MembershipManager extends AutoCloseable {

  public static final String PATH_SEPARATOR = "/";

  /**
   * An idempotent call to register to join the membership.
   * @param worker
   * @throws IOException
   */
  public void join(WorkerInfo worker) throws IOException;

  /**
   * Get all registered worker members.
   * @return all registered workers
   * @throws IOException
   */
  public List<WorkerInfo> getAllMembers() throws IOException;

  /**
   * Get healthy workers.
   * @return healthy worker list
   * @throws IOException
   */
  public List<WorkerInfo> getLiveMembers() throws IOException;

  /**
   * Get all failed workers.
   * @return failed worker list
   * @throws IOException
   */
  public List<WorkerInfo> getFailedMembers() throws IOException;

  /**
   * Pretty printed members and its liveness status.
   * @return pretty-printed status string
   */
  public String showAllMembers();

  /**
   * Stop heartbeating for liveness for current worker.
   * @param worker WorkerInfo
   * @throws IOException
   */
  @VisibleForTesting
  public void stopHeartBeat(WorkerInfo worker) throws IOException;

  /**
   * Decommision a worker.
   * @param worker WorkerInfo
   * @throws IOException
   */
  public void decommission(WorkerInfo worker) throws IOException;

  /**
   * Factory class to get or create a MembershipManager.
   */
  class Factory {
    private static final Logger LOG = LoggerFactory.getLogger(Factory.class);
    private static final Lock INIT_LOCK = new ReentrantLock();
    @GuardedBy("INIT_LOCK")
    private static final AtomicReference<MembershipManager> MEMBERSHIP_MANAGER =
        new AtomicReference<>();

    /**
     * Get or create a MembershipManager instance.
     * @param conf
     * @return MembershipManager
     */
    public static MembershipManager get(AlluxioConfiguration conf) {
      if (MEMBERSHIP_MANAGER.get() == null) {
        try (LockResource lockResource = new LockResource(INIT_LOCK)) {
          if (MEMBERSHIP_MANAGER.get() == null) {
            MEMBERSHIP_MANAGER.set(create(conf));
          }
        }
      }
      return MEMBERSHIP_MANAGER.get();
    }

    /**
     * @param conf the Alluxio configuration
     * @return an instance of {@link MembershipManager}
     */
    public static MembershipManager create(AlluxioConfiguration conf) {
      switch (conf.getEnum(PropertyKey.WORKER_MEMBERSHIP_MANAGER_TYPE, MembershipType.class)) {
        case STATIC:
          return StaticMembershipManager.create(conf);
        case ETCD:
          return EtcdMembershipManager.create(conf);
        case NOOP:
          return NoOpMembershipManager.create();
        default:
          throw new IllegalStateException("Unrecognized Membership Type");
      }
    }
  }
}
