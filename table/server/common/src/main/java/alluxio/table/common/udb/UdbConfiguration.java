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

package alluxio.table.common.udb;

import alluxio.table.common.BaseConfiguration;
import alluxio.table.common.ConfigurationUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This represents a configuration of the UDB.
 */
public class UdbConfiguration extends BaseConfiguration<UdbProperty> {
  private static final Logger LOG = LoggerFactory.getLogger(UdbConfiguration.class);

  // {...} group the scheme/authority, and are not special to various shells
  private static final Pattern CONFIG_PATTERN = Pattern.compile("(\\{.*\\})\\.(.+?)");
  public static final String READ_ONLY_OPTION = "readonly";
  public static final String SHARED_OPTION = "shared";
  public static final String REGEX_PREFIX = "regex:";

  protected final Map<String, Map<String, String>> mMountOptions;

  /**
   * Creates an instance.
   *
   * @param values the map of values
   */
  public UdbConfiguration(Map<String, String> values) {
    super(values);
    mMountOptions = new HashMap<>(values.size());
    for (Map.Entry<String, String> entry : values.entrySet()) {
      if (entry.getKey().startsWith(ConfigurationUtils.MOUNT_PREFIX)) {
        String key = entry.getKey().substring(ConfigurationUtils.MOUNT_PREFIX.length());
        Matcher m = CONFIG_PATTERN.matcher(key);
        if (m.matches()) {
          // templateSchemeAuthority can be a regex string.
          String templateSchemeAuthority = m.group(1);
          String option = m.group(2);

          // remove the bracket around the scheme://authority
          templateSchemeAuthority =
              templateSchemeAuthority.substring(1, templateSchemeAuthority.length() - 1);
          if (!templateSchemeAuthority.endsWith("/")) {
            // include the trailing '/'
            templateSchemeAuthority += "/";
          }

          Map<String, String> optionMap = mMountOptions.get(templateSchemeAuthority);
          if (optionMap == null) {
            optionMap = new HashMap<>();
            optionMap.put(option, entry.getValue());
            mMountOptions.put(templateSchemeAuthority, optionMap);
          } else {
            optionMap.put(option, entry.getValue());
          }
        }
      }
    }
  }

  /**
   * Returns the mount option for a particular scheme and authority URL.
   *
   * @param concreteSchemeAuthority scheme://authority/ (expected to have a trailing '/')
   * @return mount options in a map of or matched given concreteSchemeAuthority
   */
  public Map<String, String> getMountOption(String concreteSchemeAuthority) {
    if (!concreteSchemeAuthority.endsWith("/")) {
      // include the trailing '/'
      concreteSchemeAuthority += "/";
    }
    Map<String, String> map =
        mMountOptions.getOrDefault(concreteSchemeAuthority, Collections.emptyMap());
    if (map.equals(Collections.emptyMap())) {
      for (Entry<String, Map<String, String>> entry : mMountOptions.entrySet()) {
        if (entry.getKey().startsWith(REGEX_PREFIX)
            && concreteSchemeAuthority.matches(entry.getKey().substring(REGEX_PREFIX.length()))) {
          return entry.getValue();
        }
      }
    }
    return map;
  }
}
