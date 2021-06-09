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
          String schemeAuthority = m.group(1);
          String option = m.group(2);

          // remove the bracket around the scheme://authority
          schemeAuthority = schemeAuthority.substring(1, schemeAuthority.length() - 1);
          if (!schemeAuthority.endsWith("/")) {
            // include the trailing '/'
            schemeAuthority += "/";
          }

          Map<String, String> optionMap = mMountOptions.get(schemeAuthority);
          if (optionMap == null) {
            optionMap = new HashMap<>();
            optionMap.put(option, entry.getValue());
            mMountOptions.put(schemeAuthority, optionMap);
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
   * @param schemeAuthority scheme://authority/ (expected to have a trailing '/')
   * @return mount options in a map
   */
  public Map<String, String> getMountOption(String schemeAuthority) {
    if (!schemeAuthority.endsWith("/")) {
      // include the trailing '/'
      schemeAuthority += "/";
    }
    Map<String, String> map =
        mMountOptions.getOrDefault(schemeAuthority, Collections.emptyMap());
    if (map.equals(Collections.emptyMap())) {
      for (Entry<String, Map<String, String>> entry : mMountOptions.entrySet()) {
        if (schemeAuthority.matches(entry.getKey())) {
          return entry.getValue();
        }
      }
    }
    return map;
  }
}
