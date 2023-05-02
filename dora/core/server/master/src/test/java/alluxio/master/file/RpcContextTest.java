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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import alluxio.exception.status.UnavailableException;
import alluxio.master.file.contexts.CallTracker;
import alluxio.master.file.contexts.InternalOperationContext;
import alluxio.master.file.contexts.OperationContext;
import alluxio.master.journal.JournalContext;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Unit tests for {@link RpcContext}.
 */
public final class RpcContextTest {
  private BlockDeletionContext mMockBDC = mock(BlockDeletionContext.class);
  private JournalContext mMockJC = mock(JournalContext.class);
  private OperationContext mMockOC = mock(OperationContext.class);
  private RpcContext mRpcContext;

  @Rule
  public ExpectedException mException = ExpectedException.none();

  @Before
  public void before() {
    mRpcContext = new RpcContext(mMockBDC, mMockJC, mMockOC);
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

  @Test
  public void testCallTrackers() throws Throwable {
    InternalOperationContext opCtx = new InternalOperationContext();
    // Add a call tracker that's always cancelled.
    opCtx = opCtx.withTracker(new CallTracker() {
      @Override
      public boolean isCancelled() {
        return true;
      }

      @Override
      public Type getType() {
        return Type.GRPC_CLIENT_TRACKER;
      }
    });
    // Add a call tracker that's never cancelled.
    opCtx = opCtx.withTracker(new CallTracker() {
      @Override
      public boolean isCancelled() {
        return false;
      }

      @Override
      public Type getType() {
        return Type.STATE_LOCK_TRACKER;
      }
    });
    // Create RPC context.
    RpcContext rpcCtx = new RpcContext(mMockBDC, mMockJC, opCtx);
    // Verify the RPC is cancelled due to tracker that's always cancelled.
    assertTrue(rpcCtx.isCancelled());
    // Verify cancellation throws.
    mException.expect(RuntimeException.class);
    rpcCtx.throwIfCancelled();
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
