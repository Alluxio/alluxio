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
 * For example, the persistence state uses the default {@link NamespacePrefix#SYSTEM} prefix and
 * appends {@code ps} (an acronym for persistence state) so that the attribute when stored is
 * "{@code s.ps}" which is only a 4-character long string. The reason the prefix and is so short
 * and attribute identifier are so small is to try to reduce the memory footprint of the
 * attribute when stored in the inode. Compare this to using the attribute name
 * "system.persistance_state". The second one is 6x longer and will use at least 6x more memory.
 * Over a couple hundred million inodes this difference can contribute to a significant
 * increase in memory usage.
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
