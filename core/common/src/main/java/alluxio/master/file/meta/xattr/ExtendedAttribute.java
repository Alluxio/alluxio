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

package alluxio.master.file.meta.xattr;

import java.io.IOException;

/**
 * This class defines an interface for implementing an extended attribute which can be stored on
 * inodes.
 *
 * Attributes are stored on inodes as a simple array of byte and define their implementations
 * for encoding/decoding
 *
 * @param <T> The type of object to encode/decode
 */
public interface ExtendedAttribute<T> {

  /**
   * @return the full attribute with namespace and identifier
   */
  String getName();

  /**
   * @return the namespace of the attribute lies within
   */
  String getNamespace();

  /**
   * @return the identifier within the namespace of the attribute
   */
  String getIdentifier();

  /**
   * Encode a single object into a byte string.
   *
   * @param object the object to encode
   * @return the byte representation of the object
   */
  byte[] encode(T object);

  /**
   * Decode an object from a single byte string.
   *
   * @param bytes the bytes to decode into a single instance of
   * @return the instance of the decoded object
   * @throws IOException if the size of the byte string isn't equal to the encoding length
   */
  T decode(byte[] bytes) throws IOException;

  /**
   * The namespace for extended attributes.
   *
   * The values provided in this enum are the standard namespaces for all {@code xattr}
   */
  enum NamespacePrefix {
    SECURITY("security"),
    SYSTEM("system"),
    TRUSTED("trusted"),
    USER("user"),
    ;

    private final String mPrefixName;
    NamespacePrefix(String name) {
      mPrefixName = name;
    }

    @Override
    public String toString() {
      return mPrefixName;
    }
  }
}
