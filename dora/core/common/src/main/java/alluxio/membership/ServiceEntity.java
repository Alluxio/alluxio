package alluxio.membership;

import io.etcd.jetcd.support.CloseableClient;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class ServiceEntity implements Closeable {
  private CloseableClient mKeepAliveClient;
//  private Client mEtcdClient;
  AlluxioEtcdClient.Lease mLease; // used for keep alive(heartbeating) will not be set on start up
  protected String mServiceEntityName; // user defined name for this service entity (e.g. worker-0)
  protected long mRevision;

  public ServiceEntity() {}

  public ServiceEntity(String serviceEntityName) {
    mServiceEntityName = serviceEntityName;
  }

  public String getServiceEntityName() {
    return mServiceEntityName;
  }

  public void setKeepAliveClient(CloseableClient keepAliveClient) {
    mKeepAliveClient = keepAliveClient;
  }

  public CloseableClient getKeepAliveClient() {
    return mKeepAliveClient;
  }

  public void serialize(DataOutputStream dos) throws IOException {
    dos.writeUTF(mServiceEntityName);
    dos.writeLong(mRevision);
  }

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
