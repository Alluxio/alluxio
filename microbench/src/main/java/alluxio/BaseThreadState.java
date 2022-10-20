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

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.ThreadParams;

/**
 * Captures commonly reused thread state such as ID and file name and depth to poll.
 */
@State(Scope.Thread)
public class BaseThreadState {
  public int mMyId;

  @Setup(Level.Trial)
  public void init(ThreadParams params) {
    mMyId = params.getThreadIndex();
  }

  public int nextDepth(BaseFileStructure structure) {
    return structure.mDepthGenerator.nextValue().intValue();
  }

  public int nextWidth(BaseFileStructure structure) {
    return structure.mWidthGenerator.nextValue().intValue();
  }

  public long nextFileId(BaseFileStructure structure, int depth) {
    return structure.mFileGenerators.get(depth).nextValue().longValue();
  }
}
