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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import io.etcd.jetcd.support.CloseableClient;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.concurrent.GuardedBy;

/**
 * Base Entity class including information to register to Etcd
 * when using EtcdMembershipManager.
 * It will be serialized to JSON format to store on etcd, including
 * only those fields with marked @Expose annotation.
 */
public abstract class DefaultServiceEntity implements Closeable {
  private CloseableClient mKeepAliveClient;
  // (package visibility) to do keep alive(heartbeating),
  // initialized at time of service registration
  private AlluxioEtcdClient.Lease mLease = null;
  @Expose
  @com.google.gson.annotations.SerializedName("ServiceEntityName")
  protected String mServiceEntityName; // unique service alias
  // revision number of kv pair of registered entity on etcd, used for CASupdate
  protected long mRevision;
  public final ReentrantLock mLock = new ReentrantLock();
  // For {@link ServiceDiscoveryRecipe#RetryKeepAliveObserver}
  // to act on events such as lease keepalive connection ended or errors.
  // {@link ServiceDiscoveryRecipe#checkAllForReconnect} will periodically
  // check and resume the connection.
  public AtomicBoolean mNeedReconnect = new AtomicBoolean(false);

  /**
   * CTOR for ServiceEntity with given ServiceEntity name.
   * @param serviceEntityName
   */
  public DefaultServiceEntity(String serviceEntityName) {
    mServiceEntityName = serviceEntityName;
  }

  /**
   * Get service entity name.
   * @return service entity name
   */
  public String getServiceEntityName() {
    return mServiceEntityName;
  }

  /**
   * Set keep alive client.
   * @param keepAliveClient
   */
  public void setKeepAliveClient(CloseableClient keepAliveClient) {
    mKeepAliveClient = keepAliveClient;
  }

  /**
   * Get the keepalive client instance.
   * @return jetcd keepalive client
   */
  public CloseableClient getKeepAliveClient() {
    return mKeepAliveClient;
  }

  /**
   * @return lease
   */
  public AlluxioEtcdClient.Lease getLease() {
    return mLease;
  }

  /**
   * Set lease.
   * @param lease
   */
  @GuardedBy("mLock")
  public void setLease(AlluxioEtcdClient.Lease lease) {
    mLease = lease;
  }

  /**
   * Get the revision number of currently registered DefaultServiceEntity
   * on ETCD.
   * @return revision number
   */
  public long getRevisionNumber() {
    return mRevision;
  }

  /**
   * Set revision number.
   * @param revisionNumber
   */
  public void setRevisionNumber(long revisionNumber) {
    mRevision = revisionNumber;
  }

  /**
   * @return lock for atomically modifying certain fields
   */
  ReentrantLock getLock() {
    return mLock;
  }

  /**
   * Convert a WorkerServiceEntity into a json string.
   * @param entity
   * @return json string
   */
  public static String toJson(DefaultServiceEntity entity) {
    Gson gson = new GsonBuilder()
        .excludeFieldsWithoutExposeAnnotation()
        .create();
    return gson.toJson(entity);
  }

  @Override
  public void close() throws IOException {
    if (mKeepAliveClient != null) {
      mKeepAliveClient.close();
    }
  }
}
