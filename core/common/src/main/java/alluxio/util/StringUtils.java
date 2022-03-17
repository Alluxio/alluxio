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

package alluxio.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Utilities.
 */
public class StringUtils {

  /**
   * Convert a string into a collection after delimit it.
   *
   * @param str a string to be parsed
   * @param delim the delimiters
   * @return the collection that delimited
   */
  public static Collection<String> getStringCollection(String str, String delim) {
    List<String> values = new ArrayList();
    if (str == null) {
      return values;
    } else {
      StringTokenizer tokenizer = new StringTokenizer(str, delim);

      while (tokenizer.hasMoreTokens()) {
        values.add(tokenizer.nextToken());
      }
      return values;
    }
  }
}
