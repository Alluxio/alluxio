/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master.next.serialize;

import java.io.IOException;
import java.io.OutputStream;

/**
 * For each class that needs serialization ability, it should hide the logic of (de)serialization
 * in an implementation of this interface, the implementation is often a static inner class.
 *
 * Although there is no general method for deserialization in this interface because interface for
 * deserialization is implementation related, it is recommended to put deserialization logic as a
 * static method in the implementation of this interface for easy maintainence.
 *
 * @param <T> type of the class that needs serialization ability
 */
public interface Serializer<T> {
  void serialize(T o, OutputStream os) throws IOException;
}
