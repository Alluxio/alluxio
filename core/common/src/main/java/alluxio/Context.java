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

package alluxio;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility class for fetching configuration across different types of Alluxio processes.
 */
@ThreadSafe
public final class Context {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static Configuration sConfiguration = null;

  /**
   * @return the configuration retrieved from the context of the current process
   */
  public static synchronized Configuration getConf() {
    if (sConfiguration == null) {
      try {
        Class clazz;
        switch (AlluxioProcess.getType()) {
          case MASTER:
            clazz = Class.forName("alluxio.master.MasterContext");
            break;
          case WORKER:
            clazz = Class.forName("alluxio.worker.WorkerContext");
            break;
          case CLIENT:
            clazz = Class.forName("alluxio.client.ClientContext");
            break;
          default:
            throw new IllegalStateException("unexpected process type");
        }
        Method method = clazz.getMethod("getConf");
        sConfiguration = (Configuration) method.invoke(null);
      } catch (Exception e) {
        LOG.warn("failed to retrieve configuration from the context of the current process");
        Throwables.propagate(e);
      }
    }
    return sConfiguration;
  }

  private Context() {} // prevent instantiation
}
