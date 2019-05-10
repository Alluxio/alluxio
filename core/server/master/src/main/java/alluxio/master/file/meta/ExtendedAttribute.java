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

package alluxio.master.file.meta;

/**
 * This enum is used to define constant values for extended attribute keys on inodes within
 * the inode tree.
 *
 *
 *
 * Extended attributes take the form of
 * {@code {NAMESPACE}{SEPARATOR}{ATTRIBUTE_name}}
 *
 * For example, the persis
 */
public enum ExtendedAttribute {

  PERSISTENCE_STATE("ps"), // system (ufs) property
  PIN_STATE(NamespacePrefix.USER, "pin"), // property set by user
  ;

  private static final String SEPARATOR = ".";

  private final String mName;

  ExtendedAttribute(NamespacePrefix prefix, String name) {
    mName = buildAttr(prefix.toString(), name);
  }

  ExtendedAttribute(String name) {
    this(NamespacePrefix.SYSTEM, name);
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

  @Override
  public String toString() {
    return mName;
  }

  private enum NamespacePrefix {
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
