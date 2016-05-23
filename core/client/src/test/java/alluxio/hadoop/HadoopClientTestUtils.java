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

package alluxio.hadoop;

import com.google.common.base.Throwables;
import org.powermock.reflect.Whitebox;

/**
 * Utility methods for the Hadoop client tests.
 */
public final class HadoopClientTestUtils {
  /**
   * Resets the initialized flag in {@link AbstractFileSystem} allowing FileSystems with
   * different URIs to be initialized.
   */
  public static void resetHadoopClientContext() {
    try {
      Whitebox.setInternalState(AbstractFileSystem.class, "sInitialized", false);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
