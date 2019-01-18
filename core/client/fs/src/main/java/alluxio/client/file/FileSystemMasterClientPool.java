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

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.master.MasterClientConfig;
import alluxio.master.MasterInquireClient;
import alluxio.resource.ResourcePool;

import com.google.common.io.Closer;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.Subject;

/**
 * A fixed pool of FileSystemMasterClient instances.
 */
@ThreadSafe
public final class FileSystemMasterClientPool extends ResourcePool<FileSystemMasterClient> {
  private final MasterInquireClient mMasterInquireClient;
  private final Queue<FileSystemMasterClient> mClientList;
  private final Subject mSubject;
  private final AlluxioConfiguration mAlluxioConf;

  /**
   * Creates a new file system master client pool.
   *
   * @param subject the parent subject
   * @param masterInquireClient a client for determining the master address
   * @param alluxioConf Alluxio configuration
   */
  public FileSystemMasterClientPool(Subject subject, MasterInquireClient masterInquireClient,
      AlluxioConfiguration alluxioConf) {
    super(alluxioConf.getInt(PropertyKey.USER_FILE_MASTER_CLIENT_THREADS));
    mMasterInquireClient = masterInquireClient;
    mClientList = new ConcurrentLinkedQueue<>();
    mSubject = subject;
    mAlluxioConf = alluxioConf;
  }

  /**
   * Creates a new file system master client pool.
   *
   * @param subject the parent subject
   * @param masterInquireClient a client for determining the master address
   * @param clientThreads the number of client threads to use
   * @param alluxioConf Alluxio configuration
   */
  public FileSystemMasterClientPool(Subject subject, MasterInquireClient masterInquireClient,
      int clientThreads, AlluxioConfiguration alluxioConf) {
    super(clientThreads);
    mMasterInquireClient = masterInquireClient;
    mClientList = new ConcurrentLinkedQueue<>();
    mSubject = subject;
    mAlluxioConf = alluxioConf;
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
    FileSystemMasterClient client = FileSystemMasterClient.Factory.create(MasterClientConfig
        .defaults(mAlluxioConf).withSubject(mSubject).withMasterInquireClient(mMasterInquireClient),
        mAlluxioConf);
    mClientList.add(client);
    return client;
  }
}
