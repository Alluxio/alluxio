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

package alluxio.wire;

import alluxio.PropertyKey.Scope;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a configuration report which records the configuration checker results.
 * Since we check server-side configuration, Scope here only includes
 * SERVER, MASTER and WORKER. Scope.ALL will be considered as Scope.SERVER.
 */
public class ConfigCheckReport {
  /** Record the configuration errors. */
  private final Map<Scope, List<InconsistentProperty>> mConfigErrors;
  /** Record the configuration warnings. */
  private final Map<Scope, List<InconsistentProperty>> mConfigWarns;
  /** Record the overall status of config check report. */
  private final ConfigStatus mConfigStatus;

  /**
   * Creates a new instance of {@link ConfigCheckReport}.
   */
  public ConfigCheckReport() {
    mConfigErrors = new HashMap<>();
    mConfigWarns = new HashMap<>();
    mConfigStatus = ConfigStatus.NOT_STARTED;
  }

  /**
   * Creates a new instance of {@link ConfigCheckReport}.
   *
   * @param configErrors the configuration errors
   * @param configWarns the configuration warnings
   * @param ConfigStatus the overall configuration report status
   */
  public ConfigCheckReport(Map<Scope, List<InconsistentProperty>> configErrors,
      Map<Scope, List<InconsistentProperty>> configWarns, ConfigStatus ConfigStatus) {
    mConfigErrors = configErrors;
    mConfigWarns = configWarns;
    mConfigStatus = ConfigStatus;
  }

  /**
   * @return a map of configuration errors
   */
  public Map<Scope, List<InconsistentProperty>> getConfigErrors() {
    return mConfigErrors;
  }

  /**
   * @return a map of configuration warnings
   */
  public Map<Scope, List<InconsistentProperty>> getConfigWarns() {
    return mConfigWarns;
  }

  /**
   * @return the overall configuration status
   */
  public ConfigStatus getConfigStatus() {
    return mConfigStatus;
  }

  /**
   * Config status of the config check.
   */
  public enum ConfigStatus {
    PASSED, // do not have configuration errors and warnings
    WARN, // do not have configuration errors but have warnings
    FAILED, // have configuration errors
    NOT_STARTED;

    /**
     * @return the thrift representation of this configuration status field
     */
    public alluxio.thrift.ConfigStatus toThrift() {
      return alluxio.thrift.ConfigStatus.valueOf(name());
    }

    /**
     * @param field the thrift representation of the configuration status field to create
     * @return the wire type version of the configuration status field
     */
    public static ConfigStatus fromThrift(alluxio.thrift.ConfigStatus field) {
      return ConfigStatus.valueOf(field.name());
    }
  }
}
