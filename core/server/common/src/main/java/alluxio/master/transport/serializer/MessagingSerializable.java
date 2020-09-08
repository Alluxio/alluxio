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

package alluxio.master.transport.serializer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Message that is serializable.
 */
public interface MessagingSerializable {
  /**
   * Writes a object.
   *
   * @param os output stream
   */
  void writeObject(DataOutputStream os) throws IOException;

  /**
   * Reads a object.
   *
   * @param is input stream
   */
  void readObject(DataInputStream is) throws IOException, ClassNotFoundException;
}

