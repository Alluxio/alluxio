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

package alluxio.table.under.hive.util;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Proxy;

import javax.annotation.Nullable;

class HMSClientFactory {
  private static final Logger LOG = LoggerFactory.getLogger(HMSClientFactory.class);

  static IMetaStoreClient newInstance(IMetaStoreClient delegate) {
    HMSShim compatibility = null;
    try {
      compatibility = new HMSShim(delegate);
    } catch (Throwable t) {
      LOG.warn("Unable to initialize hive metastore compatibility client", t);
    }
    return newInstance(delegate, compatibility);
  }

  private static IMetaStoreClient newInstance(IMetaStoreClient delegate,
      @Nullable HMSShim compatibility) {
    ClassLoader classLoader = IMetaStoreClient.class.getClassLoader();
    Class<?>[] interfaces = new Class<?>[] { IMetaStoreClient.class };
    CompatibleMetastoreClient handler = new CompatibleMetastoreClient(delegate,
        compatibility);
    return (IMetaStoreClient) Proxy.newProxyInstance(classLoader, interfaces, handler);
  }
}
