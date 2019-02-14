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

package alluxio.master.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import alluxio.exception.status.UnavailableException;
import alluxio.master.journal.JournalContext;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Unit tests for {@link RpcContext}.
 */
public final class RpcContextTest {
  private BlockDeletionContext mMockBDC = mock(BlockDeletionContext.class);
  private JournalContext mMockJC = mock(JournalContext.class);
  private RpcContext mRpcContext;

  @Before
  public void before() {
    mRpcContext = new RpcContext(mMockBDC, mMockJC);
  }

  @Test
  public void success() throws Throwable {
    mRpcContext.close();
  }

  @Test
  public void order() throws Throwable {
    List<Object> order = new ArrayList<>();
    doAnswer(unused -> order.add(mMockJC)).when(mMockJC).close();
    doAnswer(unused -> order.add(mMockBDC)).when(mMockBDC).close();
    mRpcContext.close();
    assertEquals(Arrays.asList(mMockJC, mMockBDC), order);
  }

  @Test
  public void throwTwoRuntimeExceptions() throws Throwable {
    Exception bdcException = new IllegalStateException("block deletion context exception");
    Exception jcException = new IllegalArgumentException("journal context exception");
    doThrow(bdcException).when(mMockBDC).close();
    doThrow(jcException).when(mMockJC).close();
    try {
      mRpcContext.close();
      fail("Expected an exception to be thrown");
    } catch (RuntimeException e) {
      assertEquals(jcException, e);
      // journal context is closed first, so the block deletion context exception should be
      // suppressed.
      assertEquals(bdcException, e.getSuppressed()[0]);
    }
  }

  @Test
  public void throwTwoUnavailableExceptions() throws Throwable {
    Exception bdcException = new UnavailableException("block deletion context exception");
    Exception jcException = new UnavailableException("journal context exception");
    doThrow(bdcException).when(mMockBDC).close();
    doThrow(jcException).when(mMockJC).close();
    try {
      mRpcContext.close();
      fail("Expected an exception to be thrown");
    } catch (UnavailableException e) {
      assertEquals(jcException, e);
      // journal context is closed first, so the block deletion context exception should be
      // suppressed.
      assertEquals(bdcException, e.getSuppressed()[0]);
    }
  }

  @Test
  public void blockDeletionContextThrows() throws Throwable {
    Exception bdcException = new UnavailableException("block deletion context exception");
    doThrow(bdcException).when(mMockBDC).close();
    checkClose(bdcException);
  }

  @Test
  public void journalContextThrows() throws Throwable {
    Exception jcException = new UnavailableException("journal context exception");
    doThrow(jcException).when(mMockJC).close();
    checkClose(jcException);
  }

  /*
   * Checks that closing the rpc context throws the expected exception, then verifies that all
   * contexts have been closed.
   */
  private void checkClose(Exception expected) throws Throwable {
    try {
      mRpcContext.close();
      fail("Expected an exception to be thrown");
    } catch (Exception e) {
      assertEquals(expected, e);
      verify(mMockJC).close();
      verify(mMockBDC).close();
    }
  }
}
