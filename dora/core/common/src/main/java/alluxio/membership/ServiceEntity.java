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

import io.etcd.jetcd.support.CloseableClient;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Base Entity class including information to register to Etcd
 * when using EtcdMembershipManager.
 */
@ThreadSafe
public class ServiceEntity implements Closeable {
  private CloseableClient mKeepAliveClient;
  // (package visibility) to do keep alive(heartbeating),
  // initialized at time of service registration
  AlluxioEtcdClient.Lease mLease;
  protected String mServiceEntityName; // unique service alias
  // revision number of kv pair of registered entity on etcd, used for CASupdate
  protected long mRevision;
  public final ReentrantLock mLock = new ReentrantLock();
  public AtomicBoolean mNeedReconnect = new AtomicBoolean(false);

  /**
   * CTOR for ServiceEntity.
   */
  public ServiceEntity() {}

  /**
   * CTOR for ServiceEntity with given ServiceEntity name.
   * @param serviceEntityName
   */
  public ServiceEntity(String serviceEntityName) {
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
   * Serialize the ServiceEntity to output stream.
   * @param dos
   * @throws IOException
   */
  public void serialize(DataOutputStream dos) throws IOException {
    dos.writeUTF(mServiceEntityName);
    dos.writeLong(mRevision);
  }

  /**
   * Deserialize the ServiceEntity from input stream.
   * @param dis
   * @throws IOException
   */
  public void deserialize(DataInputStream dis) throws IOException {
    mServiceEntityName = dis.readUTF();
    mRevision = dis.readLong();
  }

  @Override
  public void close() throws IOException {
    if (mKeepAliveClient != null) {
      mKeepAliveClient.close();
    }
  }
}
