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

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Extend this class to implement any extended attributes.
 *
 * Any attributes which extend this class get a default implementation for multiEncode and
 * multiDecode interface methods.
 *
 * @param <T> The class which the attribute should encode from, and decode to
 */
public abstract class AbstractExtendedAttribute<T> implements ExtendedAttribute<T> {
  private static final String SEPARATOR = ".";

  private final NamespacePrefix mPrefix;
  private final String mIdentifier;
  private final String mFullName;
  private final int mEncodedSize;

  AbstractExtendedAttribute(NamespacePrefix prefix, String identifier, int encodedSize) {
    mPrefix = prefix;
    mIdentifier = identifier;
    mFullName = buildAttr(prefix.toString(), identifier);
    Preconditions.checkArgument(encodedSize > 0, "encoding size");
    mEncodedSize = encodedSize;
  }

  @Override
  public String getName() {
    return mFullName;
  }

  @Override
  public String getNamespace() {
    return mPrefix.toString();
  }

  @Override
  public String getIdentifier() {
    return mIdentifier;
  }

  @Override
  public int getEncodedSize() {
    return mEncodedSize;
  }

  @Override
  public ByteString multiEncode(List<T> objects) {
    byte[] b = new byte[objects.size() * mEncodedSize];
    int offset = 0;
    for (T obj: objects) {
      encode(obj).copyTo(b, offset);
      offset += mEncodedSize;
    }
    return ByteString.copyFrom(b);
  }

  @Override
  public List<T> multiDecode(ByteString bytes) throws IOException {
    if (bytes.size() % mEncodedSize != 0) {
      throw new IOException("Cannot decode attribute. Byte array is not a multiple of encoding "
          + "size");
    }
    int numObjects = bytes.size() / mEncodedSize;
    ArrayList<T> obj = new ArrayList<>(numObjects);
    byte[] tmp = new byte[mEncodedSize];
    for (int i = 0; i < numObjects; i++) {
      bytes.copyTo(tmp, i * mEncodedSize, 0, mEncodedSize);
      obj.add(decode(ByteString.copyFrom(tmp)));
    }
    return obj;
  }

  /**
   * Builds an attribute by joining namespace components together.
   *
   * @param components the components to join
   * @return the attribute joined by {@link #SEPARATOR}
   */
  private static String buildAttr(String... components) {
    return String.join(SEPARATOR, components);
  }

  enum NamespacePrefix {
    SYSTEM("s"),
    USER("u"),
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
