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

package alluxio.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.io.IOException;
import java.io.InterruptedIOException;

/**
 * Unit tests for {@link ExceptionUtils}.
 */
public final class ExceptionUtilsTest {
  @Test
  public void isInterrupted() {
    assertTrue(ExceptionUtils.isInterrupted(new InterruptedException()));
    assertTrue(ExceptionUtils.isInterrupted(new InterruptedIOException()));
    assertFalse(ExceptionUtils.isInterrupted(new IOException()));
  }

  @Test
  public void containsInterruptedException() {
    Throwable t1 = new IOException(new RuntimeException(new InterruptedException()));
    Throwable t2 = new IOException(new InterruptedIOException());
    Throwable t3 = new InterruptedException();
    Throwable t4 = new IOException(new RuntimeException(new IOException()));
    assertTrue(ExceptionUtils.containsInterruptedException(t1));
    assertTrue(ExceptionUtils.containsInterruptedException(t2));
    assertTrue(ExceptionUtils.containsInterruptedException(t3));
    assertFalse(ExceptionUtils.containsInterruptedException(t4));
  }
}
