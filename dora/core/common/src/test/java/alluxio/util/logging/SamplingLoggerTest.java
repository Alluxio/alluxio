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

package alluxio.util.logging;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import alluxio.Constants;
import alluxio.util.CommonUtils;

import org.junit.Test;
import org.slf4j.Logger;

/**
 * Unit tests for {@link SamplingLogger}.
 */
public class SamplingLoggerTest {
  private Logger mBaseLogger;
  private SamplingLogger mSamplingLogger;

  @Test
  public void sampleSingle() {
    setupLogger(10 * Constants.SECOND_MS);
    doReturn(true).when(mBaseLogger).isWarnEnabled();
    for (int i = 0; i < 10; i++) {
      mSamplingLogger.warn("warning");
    }
    verify(mBaseLogger, times(1)).warn("warning");
  }

  @Test
  public void sampleMultiple() {
    setupLogger(10 * Constants.SECOND_MS);
    doReturn(true).when(mBaseLogger).isWarnEnabled();
    for (int i = 0; i < 10; i++) {
      mSamplingLogger.warn("warning1");
      mSamplingLogger.warn("warning2");
    }
    verify(mBaseLogger, times(1)).warn("warning1");
    verify(mBaseLogger, times(1)).warn("warning2");
  }

  @Test
  public void sampleAfterCooldown() {
    setupLogger(1);
    doReturn(true).when(mBaseLogger).isWarnEnabled();
    for (int i = 0; i < 10; i++) {
      mSamplingLogger.warn("warning1");
      mSamplingLogger.warn("warning2");
      CommonUtils.sleepMs(2);
    }
    verify(mBaseLogger, times(10)).warn("warning1");
    verify(mBaseLogger, times(10)).warn("warning2");
  }

  private void setupLogger(long cooldown) {
    mBaseLogger = mock(Logger.class);
    mSamplingLogger = new SamplingLogger(mBaseLogger, cooldown);
  }
}
