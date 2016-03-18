/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.netty;

import static org.hamcrest.core.StringStartsWith.startsWith;

import alluxio.client.ClientContext;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Unit tests for @{link NettyRemoteBlockWriter}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ClientContext.class, NettyRemoteBlockWriter.class})
public final class NettyRemoteBlockWriterTest {

  private static final byte[] BYTES = new byte[]{(byte) 0xe0, 0x4f};
  private static final int OFFSET = 10;
  private static final int LENGTH = 20;
  private static final long BLOCK_ID = 100L;
  private static final long SESSION_ID = 200L;

  private InetSocketAddress mInetSocketAddress;
  private NettyRemoteBlockWriter mNettyRemoteBlockWriter;

  /**
   * Sets up the constructors before a test run.
   */
  @Before
  public void before() throws IOException {
    mInetSocketAddress = ClientContext.getMasterAddress();
    mNettyRemoteBlockWriter = PowerMockito.spy(new NettyRemoteBlockWriter());
    mNettyRemoteBlockWriter.open(mInetSocketAddress, BLOCK_ID, SESSION_ID);
  }

  /**
   * Sets a rule for {@link IOException} exception.
   */
  @Rule public ExpectedException mThrowsMessage = ExpectedException.none();

  /**
   * Tests the {@link NettyRemoteBlockWriter#open()} method.
   */
  @Test
  public void openTest() throws IOException {
    Assert.assertTrue((Boolean) Whitebox.getInternalState(mNettyRemoteBlockWriter, "mOpen"));
  }

  /**
   * Tests the reexecution of {@link NettyRemoteBlockWriter#open()} method to force exception.
   *
   * @throws IOException if {@link NettyRemoteBlockWriter#open()} method was already executed
   */
  @Test
  public void reOpenExceptionTest() throws IOException {
    mThrowsMessage.expect(IOException.class);
    mNettyRemoteBlockWriter.open(mInetSocketAddress, BLOCK_ID, SESSION_ID);
  }

  /**
   * Tests the {@link NettyRemoteBlockWriter#write()} method.
   *
   * @throws IOException if {@link NettyRemoteBlockWriter#write()} method exception message
   * starts with "java.net.ConnectException: Connection refused"
   */
  @Test()
  public void writeExceptionTest() throws IOException {
    mThrowsMessage.expect(IOException.class);
    mThrowsMessage.expectMessage(startsWith("java.net.ConnectException: Connection refused"));
    mNettyRemoteBlockWriter.write(BYTES, OFFSET, LENGTH);
    Assert.assertTrue(true);
  }

  /**
   * Tests the {@link NettyRemoteBlockWriter#write()} method.
   *
   * @throws IOException if {@link NettyRemoteBlockWriter#write()} method
   */
  @Test()
  public void writeTest() throws IOException {
    PowerMockito.doNothing().when(mNettyRemoteBlockWriter);
    mNettyRemoteBlockWriter.write(BYTES, OFFSET, LENGTH);
    Assert.assertTrue(true);
  }

  /**
   * Tests the {@link NettyRemoteBlockWriter#close()} method.
   */
  @Test
  public void closeTest() throws IOException {
    mNettyRemoteBlockWriter.close();
    Assert.assertFalse((Boolean) Whitebox.getInternalState(mNettyRemoteBlockWriter, "mOpen"));
  }
}
