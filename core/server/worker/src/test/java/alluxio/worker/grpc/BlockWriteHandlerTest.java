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

package alluxio.worker.grpc;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import alluxio.grpc.RequestType;
import alluxio.util.CommonUtils;
import alluxio.worker.block.BlockStore;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.block.io.LocalFileBlockWriter;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Unit tests for {@link BlockWriteHandler}.
 */
public final class BlockWriteHandlerTest extends AbstractWriteHandlerTest {
  private BlockWriter mBlockWriter;
  private File mFile;

  @Before
  public void before() throws Exception {
    mFile = mTestFolder.newFile();
    mBlockWriter = new LocalFileBlockWriter(mFile.getPath());
    mResponseObserver = mock(StreamObserver.class);
    BlockStore blockStore = mock(BlockStore.class);
    when(blockStore.createBlockWriter(anyLong(), anyLong())).thenReturn(mBlockWriter);
    mWriteHandler =
        new BlockWriteHandler(blockStore, mResponseObserver, mUserInfo, false);
    setupResponseTrigger();
  }

  @Test
  public void writeFailure() throws Exception {
    mWriteHandler.write(newWriteRequestCommand(0));
    mBlockWriter.close();
    mWriteHandler.write(newWriteRequest(newDataBuffer(CHUNK_SIZE)));
    waitForResponses();
    checkErrorCode(mResponseObserver, Status.Code.FAILED_PRECONDITION);
  }

  @Test
  public void getLocation() throws Exception {
    mWriteHandler.write(newWriteRequestCommand(0));
    CommonUtils.waitFor("location is not null", () -> !"null".equals(mWriteHandler.getLocation()));
    assertTrue(mWriteHandler.getLocation().startsWith("temp-block-"));
  }

  @Override
  protected RequestType getWriteRequestType() {
    return RequestType.ALLUXIO_BLOCK;
  }

  @Override
  protected InputStream getWriteDataStream() throws IOException {
    return new FileInputStream(mFile);
  }
}
