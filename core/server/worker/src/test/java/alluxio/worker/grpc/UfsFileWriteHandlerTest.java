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

import static org.junit.Assert.assertEquals;

import alluxio.AlluxioURI;
import alluxio.grpc.RequestType;
import alluxio.proto.dataserver.Protocol;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UfsManager.UfsClient;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.CreateOptions;
import alluxio.util.CommonUtils;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Unit tests for {@link UfsFileWriteHandler}.
 */
public final class UfsFileWriteHandlerTest extends AbstractWriteHandlerTest {
  private OutputStream mOutputStream;
  /** The file used to hold the data written by the test. */
  private File mFile;

  @Before
  public void before() throws Exception {
    mFile = mTestFolder.newFile();
    mOutputStream = new FileOutputStream(mFile);
    UnderFileSystem mockUfs = Mockito.mock(UnderFileSystem.class);
    UfsManager ufsManager = Mockito.mock(UfsManager.class);
    UfsClient ufsClient = new UfsClient(() -> mockUfs, AlluxioURI.EMPTY_URI);
    Mockito.when(ufsManager.get(TEST_MOUNT_ID)).thenReturn(ufsClient);
    Mockito.when(mockUfs.createNonexistingFile(Mockito.anyString(),
        Mockito.any(CreateOptions.class))).thenReturn(mOutputStream)
        .thenReturn(new FileOutputStream(mFile, true));
    mResponseObserver = Mockito.mock(StreamObserver.class);
    mWriteHandler = new UfsFileWriteHandler(ufsManager, mResponseObserver, mUserInfo);
    setupResponseTrigger();
  }

  @After
  public void after() throws Exception {
    mOutputStream.close();
  }

  @Test
  public void writeFailure() throws Exception {
    mWriteHandler.write(newWriteRequestCommand(0));
    mWriteHandler.write(newWriteRequest(newDataBuffer(CHUNK_SIZE)));
    mOutputStream.close();
    mWriteHandler.write(newWriteRequest(newDataBuffer(CHUNK_SIZE)));
    waitForResponses();
    checkErrorCode(mResponseObserver, Status.Code.UNKNOWN);
  }

  @Test
  public void getLocation() throws Exception {
    mWriteHandler.write(newWriteRequestCommand(0));
    CommonUtils.waitFor("location is not null", () -> !"null".equals(mWriteHandler.getLocation()));
    assertEquals("/test", mWriteHandler.getLocation());
  }

  @Override
  protected alluxio.grpc.WriteRequest newWriteRequestCommand(long offset) {
    Protocol.CreateUfsFileOptions createUfsFileOptions =
        Protocol.CreateUfsFileOptions.newBuilder().setUfsPath("/test").setOwner("owner")
            .setGroup("group").setMode(0).setMountId(TEST_MOUNT_ID).build();
    return super.newWriteRequestCommand(offset).toBuilder()
        .setCommand(super.newWriteRequestCommand(offset).getCommand().toBuilder()
        .setCreateUfsFileOptions(createUfsFileOptions)).build();
  }

  @Override
  protected RequestType getWriteRequestType() {
    return RequestType.UFS_FILE;
  }

  @Override
  protected InputStream getWriteDataStream() throws IOException {
    return new FileInputStream(mFile);
  }
}
