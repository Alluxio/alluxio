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

package alluxio.cli.fs.command;

import org.junit.Test;

public class LoadMetadataCommandTest {

  /**
   * Metadata not in Alluxio, LoadMetadata should
   * load the metadata into Alluxio inode.
   */
  @Test
  public void noneInAlluxio() {
    //TODO(Ce)
  }

  /**
   * Part of Meta data in Alluxio, LoadMetadata should
   * load another part into Alluxio inode.
   */
  @Test
  public void partInAlluxio() {
    //TODO(Ce)
  }

  /**
   * Old Meta data in Alluxio is not consistent with ufs,
   * LoadMetadata should update in Alluxio metadata.
   */
  @Test
  public void oldInAlluxio() {
    //TODO(Ce)
  }
}
