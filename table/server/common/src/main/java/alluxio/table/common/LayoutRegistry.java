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

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * The registry of layout implementations.
 */
public class LayoutRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(LayoutRegistry.class);

  private volatile Map<String, LayoutFactory> mFactories;

  /**
   * Creates an instance.
   */
  public LayoutRegistry() {
    mFactories = new HashMap<>();
  }

  /**
   * Refreshes the registry by service loading classes.
   */
  public void refresh() {
    Map<String, LayoutFactory> map = new HashMap<>();
    for (LayoutFactory factory : ServiceLoader
        .load(LayoutFactory.class, LayoutRegistry.class.getClassLoader())) {
      LayoutFactory existingFactory = map.get(factory.getType());
      if (existingFactory != null) {
        LOG.warn(
            "Ignoring duplicate layout type '{}' found in factory {}. Existing factory: {}",
            factory.getType(), factory.getClass(), existingFactory.getClass());
      }
      map.put(factory.getType(), factory);
    }
    mFactories = map;
    LOG.info("Registered Table Layouts: " + String.join(",", mFactories.keySet()));
  }

  /**
   * Creates a new instance of a {@link Layout}.
   *
   * @param layoutProto the proto representation of the layout
   * @return a new instance of the layout
   */
  public Layout create(alluxio.grpc.table.Layout layoutProto) {
    Map<String, LayoutFactory> map = mFactories;
    String type = layoutProto.getLayoutType();
    LayoutFactory factory = map.get(type);
    if (factory == null) {
      throw new IllegalStateException(
          String.format("LayoutFactory for type '%s' does not exist.", type));
    }
    return factory.create(layoutProto);
  }
}
