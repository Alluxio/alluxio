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

package alluxio.master.table;

import alluxio.table.common.BaseConfiguration;
import alluxio.table.common.ConfigurationUtils;
import alluxio.table.common.udb.UdbConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * This represents a configuration of the catalog.
 */
public class CatalogConfiguration extends BaseConfiguration<CatalogProperty> {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogConfiguration.class);

  CatalogConfiguration(Map<String, String> values) {
    super(values);
  }

  UdbConfiguration getUdbConfiguration(String udbType) {
    String udbPrefix = ConfigurationUtils.getUdbPrefix(udbType);
    HashMap<String, String> map = new HashMap<>(mValues.size());
    for (Map.Entry<String, String> entry : mValues.entrySet()) {
      if (entry.getKey().startsWith(udbPrefix)) {
        String key = entry.getKey().substring(udbPrefix.length());
        map.put(key, entry.getValue());
      }
    }
    return new UdbConfiguration(map);
  }
}
