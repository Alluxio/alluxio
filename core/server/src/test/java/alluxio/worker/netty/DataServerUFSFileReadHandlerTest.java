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

package alluxio.worker.netty;

import alluxio.EmbeddedChannelNoException;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.proto.dataserver.Protocol;
import alluxio.worker.file.FileSystemWorker;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ResourceLeakDetector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.FileInputStream;
import java.io.InputStream;

@RunWith(PowerMockRunner.class)
public final class DataServerUFSFileReadHandlerTest extends DataServerReadHandlerTest {
  private FileSystemWorker mFileSystemWorker;
  private InputStream mInputStream;

  @Before
  public void before() throws Exception {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.ADVANCED);
    mFileSystemWorker = PowerMockito.mock(FileSystemWorker.class);
    mChannel = new EmbeddedChannel(
        new DataServerUFSFileReadHandler(NettyExecutors.FILE_READER_EXECUTOR, mFileSystemWorker));
    mChannelNoException = new EmbeddedChannelNoException(
        new DataServerUFSFileReadHandler(NettyExecutors.FILE_READER_EXECUTOR, mFileSystemWorker));
  }

  @After
  public void after() throws Exception {
    mInputStream.close();
  }

  @Test
  public void readFailure() throws Exception {
    long fileSize = PACKET_SIZE * 10 + 1;
    populateInputFile(0, 0, fileSize - 1);
    mInputStream.close();
    mChannelNoException.writeInbound(buildReadRequest(0, fileSize));
    Object response = waitForOneResponse(mChannelNoException);
    checkReadResponse(response, Protocol.Status.Code.INTERNAL);
  }

  @Override
  protected void mockReader(long start) throws Exception {
    mInputStream = new FileInputStream(mFile);
    mInputStream.skip(start);
    PowerMockito.when(mFileSystemWorker.getUfsInputStream(Mockito.anyLong(), Mockito.anyLong()))
        .thenReturn(mInputStream);
  }

  @Override
  protected RPCProtoMessage buildReadRequest(long offset, long len) {
    Protocol.ReadRequest readRequest =
        Protocol.ReadRequest.newBuilder().setId(1L).setOffset(offset).setSessionId(1L)
            .setLength(len).setLockId(1L).setType(Protocol.RequestType.UFS_FILE).build();
    return new RPCProtoMessage(readRequest, null);
  }
}
