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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
    mConfigStatus = ConfigStatus.PASSED;
  }

  /**
   * Creates a new instance of {@link ConfigCheckReport}.
   *
   * @param errors the configuration errors
   * @param warns the configuration warnings
   * @param status the configuration check status
   */
  public ConfigCheckReport(Map<Scope, List<InconsistentProperty>> errors,
      Map<Scope, List<InconsistentProperty>> warns, ConfigStatus status) {
    mConfigErrors = errors;
    mConfigWarns = warns;
    mConfigStatus = status;
  }

  /**
   * Creates a new instance of {@link ConfigCheckReport} from a thrift representation.
   *
   * @param configCheckReport the thrift representation of a configuration check report
   */
  public ConfigCheckReport(alluxio.thrift.ConfigCheckReport configCheckReport) {
    mConfigErrors = new HashMap<>();
    for (Map.Entry<alluxio.thrift.Scope, List<alluxio.thrift.InconsistentProperty>> entry :
        configCheckReport.getErrors().entrySet()) {
      mConfigErrors.put(Scope.fromThrift(entry.getKey()), entry.getValue().stream()
          .map(InconsistentProperty::fromThrift).collect(Collectors.toList()));
    }
    mConfigWarns = new HashMap<>();
    for (Map.Entry<alluxio.thrift.Scope, List<alluxio.thrift.InconsistentProperty>> entry :
        configCheckReport.getWarns().entrySet()) {
      mConfigWarns.put(Scope.fromThrift(entry.getKey()), entry.getValue().stream()
          .map(InconsistentProperty::fromThrift).collect(Collectors.toList()));
    }
    mConfigStatus = ConfigStatus.fromThrift(configCheckReport.getStatus());
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
   * @return thrift representation of the configuration check report
   */
  public alluxio.thrift.ConfigCheckReport toThrift() {
    Map<alluxio.thrift.Scope, List<alluxio.thrift.InconsistentProperty>> thriftErrors
        = new HashMap<>();
    for (Map.Entry<Scope, List<InconsistentProperty>> entry :
        mConfigErrors.entrySet()) {
      thriftErrors.put(entry.getKey().toThrift(), entry.getValue().stream()
          .map(InconsistentProperty::toThrift).collect(Collectors.toList()));
    }

    Map<alluxio.thrift.Scope, List<alluxio.thrift.InconsistentProperty>> thriftWarns
        = new HashMap<>();
    for (Map.Entry<Scope, List<InconsistentProperty>> entry :
        mConfigWarns.entrySet()) {
      thriftWarns.put(entry.getKey().toThrift(), entry.getValue().stream()
          .map(InconsistentProperty::toThrift).collect(Collectors.toList()));
    }

    return new alluxio.thrift.ConfigCheckReport().setErrors(thriftErrors)
        .setWarns(thriftWarns).setStatus(mConfigStatus.toThrift());
  }

  /**
   * Creates a new instance of {@link ConfigCheckReport} from thrift representation.
   *
   * @param report the thrift representation of a configuration check report
   * @return the instance
   */
  public static ConfigCheckReport fromThrift(alluxio.thrift.ConfigCheckReport report) {
    return new ConfigCheckReport(report);
  }

  /**
   * Config status of the config check.
   */
  public enum ConfigStatus {
    PASSED, // do not have configuration errors and warnings
    WARN, // do not have configuration errors but have warnings
    FAILED; // have configuration errors

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
