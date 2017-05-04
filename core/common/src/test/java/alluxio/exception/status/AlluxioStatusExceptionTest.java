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

package alluxio.exception.status;

import static org.junit.Assert.assertSame;

import org.junit.Test;

import java.io.IOException;

/**
 * Tests for {@link AlluxioStatusException}.
 */
public final class AlluxioStatusExceptionTest {

  @Test
  public void recoverIOException() {
    IOException e = new IOException("test");
    assertSame(e, AlluxioStatusException.fromIOException(e).toIOException());
  }
}
