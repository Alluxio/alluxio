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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.Configuration;
import alluxio.PropertyKey;

import org.apache.curator.utils.ThreadUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Unit tests for {@link SafeMode}.
 */
public class SafeModeTest {
  private static final String SAFEMODE_WAIT_TEST = "1sec";

  private SafeMode mSafeMode;
  private ScheduledExecutorService mScheduledExecutorService;

  /** The exception expected to be thrown. */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  /**
   * Sets up the dependencies before a test runs.
   */
  @Before
  public void before() throws Exception {
    Configuration.set(PropertyKey.MASTER_SAFEMODE_WAIT, SAFEMODE_WAIT_TEST);
    mScheduledExecutorService =
        ThreadUtils.newSingleThreadScheduledExecutor("TestSafeMode");
    mSafeMode = new DefaultSafeMode(mScheduledExecutorService);
  }

  /**
   * Stops the master after a test ran.
   */
  @After
  public void after() throws Exception {
    mScheduledExecutorService.shutdown();
  }

  @Test
  public void defaultSafeMode() throws Exception {
    assertFalse(mSafeMode.isInSafeMode());
  }

  @Test
  public void enterSafeMode() throws Exception {
    mSafeMode.enterSafeMode();
    assertTrue(mSafeMode.isInSafeMode());
  }

  @Test
  public void leaveSafeMode() throws Exception {
    mSafeMode.enterSafeMode();
    assertTrue(mSafeMode.isInSafeMode());
    Thread.sleep(Configuration.getMs(PropertyKey.MASTER_SAFEMODE_WAIT) + 500);
    assertFalse(mSafeMode.isInSafeMode());
  }

  @Test
  public void reenterSafeMode() throws Exception {
    mSafeMode.enterSafeMode();
    assertTrue(mSafeMode.isInSafeMode());
    Thread.sleep(Configuration.getMs(PropertyKey.MASTER_SAFEMODE_WAIT) + 500);
    assertFalse(mSafeMode.isInSafeMode());
    mSafeMode.enterSafeMode();
    assertTrue(mSafeMode.isInSafeMode());
  }

  @Test
  public void reenterSafeModeWhileInSafeMode() throws Exception {
    mSafeMode.enterSafeMode();
    assertTrue(mSafeMode.isInSafeMode());

    // Enters safe mode again while in safe mode.
    Thread.sleep(Configuration.getMs(PropertyKey.MASTER_SAFEMODE_WAIT) - 100);
    assertTrue(mSafeMode.isInSafeMode());
    mSafeMode.enterSafeMode();

    // Verifies safe mode timer is reset
    assertTrue(mSafeMode.isInSafeMode());
    Thread.sleep(Configuration.getMs(PropertyKey.MASTER_SAFEMODE_WAIT) - 100);
    assertTrue(mSafeMode.isInSafeMode());
    Thread.sleep(500);
    assertFalse(mSafeMode.isInSafeMode());
  }
}