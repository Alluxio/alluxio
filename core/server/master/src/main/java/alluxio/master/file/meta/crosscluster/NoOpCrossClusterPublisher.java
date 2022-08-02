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

package alluxio.master.file.meta.crosscluster;

import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;

/**
 * A Cross Cluster Publisher that only does no ops.
 */
@VisibleForTesting
public class NoOpCrossClusterPublisher implements CrossClusterPublisher {

  @Override
  public void publish(String ufsPath) {
    // noop
  }

  @Override
  public void close() throws IOException {}
}
