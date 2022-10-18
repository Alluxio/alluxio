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

package alluxio.master;

import static org.junit.Assert.assertEquals;

import alluxio.Constants;
import alluxio.grpc.NodeState;
import alluxio.util.interfaces.Scoped;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for functionality of {@link AbstractPrimarySelector}.
 */
public final class AbstractPrimarySelectorTest {
  private static final int TIMEOUT = 10 * Constants.SECOND_MS;

  private TestSelector mSelector;
  private ScheduledExecutorService mExecutor;

  @Before
  public void before() {
    mSelector = new TestSelector();
    mExecutor = Executors.newSingleThreadScheduledExecutor();
  }

  @After
  public void after() {
    mExecutor.shutdownNow();
  }

  @Test
  public void getState() {
    assertEquals(NodeState.STANDBY, mSelector.getStateUnsafe());
    assertEquals(NodeState.STANDBY, mSelector.getState());
    mSelector.setState(NodeState.PRIMARY);
    assertEquals(NodeState.PRIMARY, mSelector.getStateUnsafe());
    assertEquals(NodeState.PRIMARY, mSelector.getState());
    mSelector.setState(NodeState.STANDBY);
    assertEquals(NodeState.STANDBY, mSelector.getStateUnsafe());
    assertEquals(NodeState.STANDBY, mSelector.getState());
  }

  @Test(timeout = TIMEOUT)
  public void waitFor() throws Exception {
    mExecutor.schedule(() -> mSelector.setState(NodeState.PRIMARY), 30, TimeUnit.MILLISECONDS);
    mSelector.waitForState(NodeState.PRIMARY);
    assertEquals(NodeState.PRIMARY, mSelector.getState());
    assertEquals(NodeState.PRIMARY, mSelector.getStateUnsafe());
    mExecutor.schedule(() -> mSelector.setState(NodeState.STANDBY), 30, TimeUnit.MILLISECONDS);
    mSelector.waitForState(NodeState.STANDBY);
    assertEquals(NodeState.STANDBY, mSelector.getStateUnsafe());
    assertEquals(NodeState.STANDBY, mSelector.getState());
  }

  @Test(timeout = TIMEOUT)
  public void onStateChange() {
    AtomicInteger primaryCounter = new AtomicInteger(0);
    AtomicInteger standbyCounter = new AtomicInteger(0);
    Scoped listener = mSelector.onStateChange(state -> {
      if (state.equals(NodeState.PRIMARY)) {
        primaryCounter.incrementAndGet();
      } else {
        standbyCounter.incrementAndGet();
      }
    });
    for (int i = 0; i < 10; i++) {
      mSelector.setState(NodeState.PRIMARY);
      mSelector.setState(NodeState.STANDBY);
    }
    assertEquals(10, primaryCounter.get());
    assertEquals(10, standbyCounter.get());
    listener.close();
    mSelector.setState(NodeState.PRIMARY);
    mSelector.setState(NodeState.STANDBY);
    assertEquals(10, primaryCounter.get());
    assertEquals(10, standbyCounter.get());
  }

  static class TestSelector extends AbstractPrimarySelector {
    @Override
    public void start(InetSocketAddress localAddress) {}

    @Override
    public void stop() {}
  }
}
