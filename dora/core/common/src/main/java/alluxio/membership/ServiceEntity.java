package alluxio.membership;

import io.etcd.jetcd.support.CloseableClient;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Base Entity class including information to register to Etcd
 * when using EtcdMembershipManager
 */
public class ServiceEntity implements Closeable {
  private CloseableClient mKeepAliveClient;
  AlluxioEtcdClient.Lease mLease; // used for keep alive(heartbeating) will not be set on start up
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

  public CloseableClient getKeepAliveClient() {
    return mKeepAliveClient;
  }

  /**
   * Serialize the ServiceEntity to output stream
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
