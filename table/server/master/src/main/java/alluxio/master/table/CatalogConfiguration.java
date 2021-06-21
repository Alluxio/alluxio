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
import alluxio.table.common.udb.UdbBypassSpec;
import alluxio.table.common.udb.UdbConfiguration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nullable;

/**
 * This represents a configuration of the catalog.
 */
public class CatalogConfiguration extends BaseConfiguration<CatalogProperty> {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogConfiguration.class);

  CatalogConfiguration(Map<String, String> values) {
    super(values);
  }

  UdbConfiguration getUdbConfiguration(String udbType) throws IOException {
    String udbPrefix = ConfigurationUtils.getUdbPrefix(udbType);
    HashMap<String, String> map = new HashMap<>(mValues.size());
    for (Map.Entry<String, String> entry : mValues.entrySet()) {
      if (entry.getKey().startsWith(udbPrefix)) {
        String key = entry.getKey().substring(udbPrefix.length());
        map.put(key, entry.getValue());
      }
    }
    return new UdbConfiguration(map, getUdbBypassSpec());
  }

  /**
   * Return configuration specified in the config file for the UDB.
   * This reads from the config file each time it is called.
   *
   * @return the {@link DbConfig} for the UDB, null if none is specified
   * @throws IOException when the config file does not exist, or has invalid syntax
   */
  @Nullable
  DbConfig getDbConfig() throws IOException {
    String configPath = get(CatalogProperty.DB_CONFIG_FILE);
    if (configPath.equals(CatalogProperty.DB_CONFIG_FILE.getDefaultValue())) {
      // no config file is specified
      return null;
    }
    if (!Files.exists(Paths.get(configPath))) {
      throw new FileNotFoundException(configPath);
    }
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.readValue(new File(configPath), DbConfig.class);
    } catch (JsonProcessingException e) {
      LOG.error("Failed to deserialize UDB config file {}", configPath, e);
      throw e;
    }
  }

  /**
   * Return the bypassing specification for the UDB from its {@link DbConfig}.
   * An empty spec is returned in case no config file is given.
   *
   * @return UDB bypassing specification
   */
  UdbBypassSpec getUdbBypassSpec() throws IOException {
    DbConfig config = getDbConfig();
    if (config == null) {
      return UdbBypassSpec.empty();
    }
    return config.getUdbBypassSpec();
  }
}
