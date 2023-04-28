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

/**
 * Extend this class to implement any extended attributes.
 *
 * @param <T> The type which the attribute should encode from, and decode to
 */
public abstract class AbstractExtendedAttribute<T> implements ExtendedAttribute<T> {
  private static final String SEPARATOR = ".";

  private final NamespacePrefix mPrefix;
  private final String mIdentifier;
  private final String mFullName;

  AbstractExtendedAttribute(NamespacePrefix prefix, String identifier) {
    mPrefix = prefix;
    mIdentifier = identifier;
    mFullName = buildAttr(prefix.toString(), identifier);
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

  /**
   * Builds an attribute by joining namespace components together.
   *
   * @param components the components to join
   * @return the attribute joined by {@link #SEPARATOR}
   */
  private static String buildAttr(String... components) {
    return String.join(SEPARATOR, components);
  }
}
