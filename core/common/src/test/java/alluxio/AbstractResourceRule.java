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

package alluxio;

import com.google.common.base.Throwables;
import org.junit.rules.ExternalResource;

import java.io.Closeable;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A rule for test rule to set and restore an external resource for each test case.
 */
@NotThreadSafe
public abstract class AbstractResourceRule extends ExternalResource {

  /**
   * @return a Closeable resource that makes the modification of the resource on construction and
   *         restore the rule to the previous value on close.
   */
  public Closeable toResource() {
    return new Closeable() {
      {
        try {
          before();
        } catch (Throwable t) {
          Throwables.propagateIfPossible(t);
          throw new RuntimeException(t);
        }
      }

      @Override
      public void close() {
        after();
      }
    };
  }
}
