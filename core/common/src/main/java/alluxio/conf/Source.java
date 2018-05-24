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

package alluxio.conf;

/**
 * The source of a configuration property.
 */
public enum Source {
  /**
   * The unknown source which has the lowest priority.
   */
  UNKNOWN,
  /**
   * The default property value from <code>PropertyKey</code> on compile time.
   */
  DEFAULT,
  /**
   * The property value is specified in site properties file (alluxio-site.properties).
   */
  SITE_PROPERTY,
  /**
   * The property value is specified with JVM -D options before passed to Alluxio.
   */
  SYSTEM_PROPERTY,
  /**
   * The property value is set by user during runtime (e.g., Configuration.set or through
   * HadoopConf). This source has the highest priority.
   */
  RUNTIME,
}
