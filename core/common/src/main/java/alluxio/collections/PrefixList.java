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

package alluxio.collections;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang.Validate;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
/**
 * Prefix list is used to do file filtering.
 */
@ThreadSafe
public final class PrefixList {
  private final List<String> mInnerList;

  /**
   * Prefix list is used to do file filtering.
   *
   * @param prefixList the list of prefixes to create
   */
  public PrefixList(List<String> prefixList) {
    if (prefixList == null) {
      mInnerList = new ArrayList<String>(0);
    } else {
      mInnerList = prefixList;
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
    mInnerList = new ArrayList<String>(0);
    if (prefixes != null && !prefixes.trim().isEmpty()) {
      String[] candidates = prefixes.trim().split(separator);
      for (String prefix : candidates) {
        String trimmed = prefix.trim();
        if (!trimmed.isEmpty()) {
          mInnerList.add(trimmed);
        }
      }
    }
  }

  /**
   * Gets the list of prefixes.
   *
   * @return the list of prefixes
   */
  public List<String> getList() {
    return ImmutableList.copyOf(mInnerList);
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
   * @return the string representation like "a;b/c"
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
