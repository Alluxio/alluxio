package alluxio.worker.membership;

import alluxio.MembershipType;
import alluxio.client.file.cache.CacheManager;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.resource.LockResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public interface MembershipManager extends AutoCloseable {

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
     * @return an instance of {@link CacheManager}
     */
    public static MembershipManager create(AlluxioConfiguration conf) throws IOException {
      switch (conf.getEnum(PropertyKey.WORKER_MEMBERSHIP_TYPE, MembershipType.class)) {
        case STATIC:
//          return new StaticMembershipManager(conf);
        case ETCD:
          return new EtcdMembershipManager(conf);
        default:
          throw new IOException("Unrecognized Membership Type.");
      }
    }
  }

}
