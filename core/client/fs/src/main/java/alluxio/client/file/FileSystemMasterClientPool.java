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

package alluxio.client.file;

import alluxio.ClientContext;
import alluxio.conf.PropertyKey;
import alluxio.master.MasterClientContext;
import alluxio.master.MasterInquireClient;
import alluxio.resource.ResourcePool;

import com.google.common.io.Closer;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A fixed pool of FileSystemMasterClient instances.
 */
@ThreadSafe
public final class FileSystemMasterClientPool extends ResourcePool<FileSystemMasterClient> {
  private final Queue<FileSystemMasterClient> mClientList;
  private final MasterClientContext mMasterContext;

  /**
   * Creates a new file system master client pool.
   *
   * @param context information for connecting to processes in the cluster
   * @param masterInquireClient a client for determining the master address
   */
  public FileSystemMasterClientPool(ClientContext context,
      MasterInquireClient masterInquireClient) {
    super(context.getConf().getInt(PropertyKey.USER_FILE_MASTER_CLIENT_THREADS));
    mClientList = new ConcurrentLinkedQueue<>();
    mMasterContext = MasterClientContext.newBuilder(context)
            .setMasterInquireClient(masterInquireClient).build();
  }

  @Override
  public void close() throws IOException {
    FileSystemMasterClient client;
    Closer closer = Closer.create();
    while ((client = mClientList.poll()) != null) {
      closer.register(client);
    }
    closer.close();
  }

  @Override
  protected FileSystemMasterClient createNewResource() {
    FileSystemMasterClient client = FileSystemMasterClient.Factory.create(mMasterContext);
    mClientList.add(client);
    return client;
  }
}
