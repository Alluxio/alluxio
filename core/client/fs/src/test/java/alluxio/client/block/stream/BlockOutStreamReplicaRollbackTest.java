/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as per the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.block.stream;

import static org.junit.Assert.assertThrows;

import alluxio.wire.WorkerNetAddress;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.Arrays;

/**
 * Tests the resource handling of {@link BlockOutStream#createReplicatedBlockOutStream}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({DataWriter.Factory.class})
public class BlockOutStreamReplicaRollbackTest {

  /**
   * When a later replica writer cannot be created, every writer that was already
   * created must be cancelled and closed instead of leaking its client and write
   * session.
   */
  @Test
  public void partialReplicaCreationCancelsCreatedWriters() throws Exception {
    WorkerNetAddress workerA = new WorkerNetAddress().setHost("worker-a").setDataPort(1);
    WorkerNetAddress workerB = new WorkerNetAddress().setHost("worker-b").setDataPort(1);
    DataWriter firstWriter = Mockito.mock(DataWriter.class);

    PowerMockito.mockStatic(DataWriter.Factory.class);
    PowerMockito.when(DataWriter.Factory.create(ArgumentMatchers.any(),
            ArgumentMatchers.anyLong(), ArgumentMatchers.anyLong(),
            ArgumentMatchers.eq(workerA), ArgumentMatchers.any()))
        .thenReturn(firstWriter);
    PowerMockito.when(DataWriter.Factory.create(ArgumentMatchers.any(),
            ArgumentMatchers.anyLong(), ArgumentMatchers.anyLong(),
            ArgumentMatchers.eq(workerB), ArgumentMatchers.any()))
        .thenThrow(new IOException("injected replica creation failure"));

    assertThrows(IOException.class, () -> BlockOutStream.createReplicatedBlockOutStream(
        null, 1L, 1024L, Arrays.asList(workerA, workerB), null));

    Mockito.verify(firstWriter).cancel();
    Mockito.verify(firstWriter).close();
  }
}
