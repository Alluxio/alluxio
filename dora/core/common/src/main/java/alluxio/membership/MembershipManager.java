package alluxio.membership;

import alluxio.MembershipType;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.resource.LockResource;
import alluxio.wire.WorkerInfo;

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
    private static final AtomicReference<MembershipManager> MEMBERSHIP_MANAGER = new AtomicReference<>();

    public static MembershipManager get(AlluxioConfiguration conf) throws IOException {
      if (MEMBERSHIP_MANAGER.get() == null) {
        try (LockResource lockResource = new LockResource(INIT_LOCK)) {
          if (MEMBERSHIP_MANAGER.get() == null) {
            MEMBERSHIP_MANAGER.set(create(conf));
          }
        } catch (IOException ex) {
          LOG.error("Failed to create MembershipManager : ", ex);
          throw ex;
        }
      }
      return MEMBERSHIP_MANAGER.get();
    }

    /**
     * @param conf the Alluxio configuration
     * @return an instance of {@link MembershipManager}
     */
    public static MembershipManager create(AlluxioConfiguration conf) throws IOException {
      switch (conf.getEnum(PropertyKey.WORKER_MEMBERSHIP_TYPE, MembershipType.class)) {
        case STATIC:
          return new StaticMembershipManager(conf);
        case ETCD:
          return new EtcdMembershipManager(conf);
        default:
          throw new IOException("Unrecognized Membership Type.");
      }
    }
  }
}
