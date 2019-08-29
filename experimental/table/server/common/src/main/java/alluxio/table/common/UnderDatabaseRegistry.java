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

package alluxio.table.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * The registry of under database implementations.
 */
public class UnderDatabaseRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(UnderDatabaseRegistry.class);

  private volatile Map<String, UnderDatabaseFactory> mFactories;

  /**
   * Creates an instance.
   */
  public UnderDatabaseRegistry() {
    mFactories = new HashMap<>();
  }

  /**
   * Refreshes the registry by service loading classes.
   */
  public void refresh() {
    Map<String, UnderDatabaseFactory> map = new HashMap<>();
    for (UnderDatabaseFactory factory : ServiceLoader
        .load(UnderDatabaseFactory.class, UnderDatabaseRegistry.class.getClassLoader())) {
      UnderDatabaseFactory existingFactory = map.get(factory.getType());
      if (existingFactory != null) {
        LOG.warn(
            "Ignoring duplicate under database type '{}' found in factory {}. Existing factory: {}",
            factory.getType(), factory.getClass(), existingFactory.getClass());
      }
      map.put(factory.getType(), factory);
    }
    mFactories = map;
  }

  /**
   * Creates a new instance of an {@link UnderDatabase}.
   *
   * @param udbContext the db context
   * @param type the udb type
   * @param configuration the udb configuration
   * @return a new udb instance
   */
  public UnderDatabase create(UdbContext udbContext, String type,
      Map<String, String> configuration) throws IOException {
    Map<String, UnderDatabaseFactory> map = mFactories;
    UnderDatabaseFactory factory = map.get(type);
    if (factory == null) {
      throw new IOException(String.format("UdbFactory for type '%s' does not exist.", type));
    }
    return factory.create(udbContext, configuration);
  }
}
