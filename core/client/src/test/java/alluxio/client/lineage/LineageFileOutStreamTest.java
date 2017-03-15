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

package alluxio.client.lineage;

import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.resource.DummyCloseableResource;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Tests {@link LineageFileOutStream}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class, FileSystemMasterClient.class})
public final class LineageFileOutStreamTest {

  @Test
  public void persistHandledByMaster() throws Exception {
    FileSystemContext context = PowerMockito.mock(FileSystemContext.class);
    FileSystemMasterClient
        client = PowerMockito.mock(FileSystemMasterClient.class);
    Mockito.when(context.acquireMasterClientResource())
        .thenReturn(new DummyCloseableResource<>(client));

    LineageFileOutStream stream = new LineageFileOutStream(context, new AlluxioURI("/path"),
        OutStreamOptions.defaults().setWriteType(WriteType.ASYNC_THROUGH));
    stream.close();
    // The lineage file out stream doesn't manage asynchronous persistence.
    Mockito.verify(client, Mockito.times(0)).scheduleAsyncPersist(new AlluxioURI("/path"));
  }
}
