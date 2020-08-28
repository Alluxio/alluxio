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

package alluxio.master.backup;

import com.google.common.base.MoreObjects;
import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.Serializer;

/**
 * The backup message used for signaling follower to suspend its journals.
 */
public class BackupSuspendMessage implements CatalystSerializable {
  /**
   * Empty constructor as per deserialization requirement.
   */
  public BackupSuspendMessage() {}

  @Override
  public void writeObject(BufferOutput<?> bufferOutput, Serializer serializer) {}

  @Override
  public void readObject(BufferInput<?> bufferInput, Serializer serializer) {}

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).toString();
  }
}
