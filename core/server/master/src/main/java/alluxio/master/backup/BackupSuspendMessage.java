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

import alluxio.master.transport.serializer.MessagingSerializable;

import com.google.common.base.MoreObjects;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * The backup message used for signaling follower to suspend its journals.
 */
public class BackupSuspendMessage implements MessagingSerializable {
  /**
   * Empty constructor as per deserialization requirement.
   */
  public BackupSuspendMessage() {}

  @Override
  public void writeObject(DataOutputStream os) throws IOException {}

  @Override
  public void readObject(DataInputStream is) throws IOException {}

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).toString();
  }
}
