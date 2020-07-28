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

package alluxio.collections;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.Validate;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;
/**
 * Prefix list is used to do file filtering.
 */
@ThreadSafe
public final class PrefixList {
  private final ImmutableList<String> mInnerList;

  /**
   * Prefix list is used to do file filtering.
   *
   * @param prefixList the list of prefixes to create
   */
  public PrefixList(List<String> prefixList) {
    if (prefixList == null) {
      mInnerList = ImmutableList.of();
    } else {
      mInnerList = ImmutableList.copyOf(prefixList);
    }
  }

  /**
   * Prefix list is used to do file filtering.
   *
   * @param prefixes the prefixes with separators
   * @param separator the separator to split the prefixes
   */
  public PrefixList(String prefixes, String separator) {
    Validate.notNull(separator);
    List<String> prefixList = new ArrayList<>(0);
    if (prefixes != null && !prefixes.trim().isEmpty()) {
      String[] candidates = prefixes.trim().split(separator);
      for (String prefix : candidates) {
        String trimmed = prefix.trim();
        if (!trimmed.isEmpty()) {
          prefixList.add(trimmed);
        }
      }
    }
    mInnerList = ImmutableList.copyOf(prefixList);
  }

  /**
   * Gets the list of prefixes.
   *
   * @return the list of prefixes
   */
  public ImmutableList<String> getList() {
    return mInnerList;
  }

  /**
   * Checks whether a prefix of {@code path} is in the prefix list.
   *
   * @param path the path to check
   * @return true if the path is in the list, false otherwise
   */
  public boolean inList(String path) {
    if (Strings.isNullOrEmpty(path)) {
      return false;
    }

    for (String prefix : mInnerList) {
      if (path.startsWith(prefix)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Checks whether a prefix of {@code path} is not in the prefix list.
   *
   * @param path the path to check
   * @return true if the path is not in the list, false otherwise
   */
  public boolean outList(String path) {
    return !inList(path);
  }

  /**
   * Print out all prefixes separated by ";".
   *
   * @return the string representation like "a;b/c;"
   */
  @Override
  public String toString() {
    StringBuilder s = new StringBuilder();
    for (String prefix : mInnerList) {
      s.append(prefix).append(";");
    }
    return s.toString();
  }
}
