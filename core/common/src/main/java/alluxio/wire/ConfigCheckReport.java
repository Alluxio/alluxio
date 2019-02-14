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

import alluxio.grpc.ConfigStatus;
import alluxio.grpc.InconsistentProperties;
import alluxio.grpc.Scope;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Represents a configuration report which records the configuration checker results. Since we check
 * server-side configuration, Scope here only includes SERVER, MASTER and WORKER. Scope.ALL will be
 * considered as Scope.SERVER.
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
   * Creates a new instance of {@link ConfigCheckReport} from a proto representation.
   *
   * @param configCheckReport the proto representation of a configuration check report
   */
  public ConfigCheckReport(alluxio.grpc.ConfigCheckReport configCheckReport) {
    mConfigErrors = new HashMap<>();
    for (Map.Entry<String, InconsistentProperties> entry : configCheckReport.getErrorsMap()
        .entrySet()) {
      mConfigErrors.put(Scope.valueOf(entry.getKey()), entry.getValue().getPropertiesList().stream()
          .map(InconsistentProperty::fromProto).collect(Collectors.toList()));
    }
    mConfigWarns = new HashMap<>();
    for (Map.Entry<String, InconsistentProperties> entry : configCheckReport.getWarnsMap()
        .entrySet()) {
      mConfigWarns.put(Scope.valueOf(entry.getKey()), entry.getValue().getPropertiesList().stream()
          .map(InconsistentProperty::fromProto).collect(Collectors.toList()));
    }
    mConfigStatus = configCheckReport.getStatus();
  }

  /**
   * Creates a new instance of {@link ConfigCheckReport} from proto representation.
   *
   * @param report the proto representation of a configuration check report
   * @return the instance
   */
  public static ConfigCheckReport fromProto(alluxio.grpc.ConfigCheckReport report) {
    return new ConfigCheckReport(report);
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
   * @return proto representation of the configuration check report
   */
  public alluxio.grpc.ConfigCheckReport toProto() {
    Map<String, InconsistentProperties> protoErrors = new HashMap<>();
    for (Map.Entry<Scope, List<InconsistentProperty>> entry : mConfigErrors.entrySet()) {
      protoErrors.put(entry.getKey().name(), InconsistentProperties.newBuilder().addAllProperties(
          entry.getValue().stream().map(InconsistentProperty::toProto).collect(Collectors.toList()))
          .build());
    }
    Map<String, InconsistentProperties> protoWarns = new HashMap<>();
    for (Map.Entry<Scope, List<InconsistentProperty>> entry : mConfigWarns.entrySet()) {
      protoWarns.put(entry.getKey().name(), InconsistentProperties.newBuilder().addAllProperties(
          entry.getValue().stream().map(InconsistentProperty::toProto).collect(Collectors.toList()))
          .build());
    }

    return alluxio.grpc.ConfigCheckReport.newBuilder().putAllErrors(protoErrors)
        .putAllWarns(protoWarns).setStatus(mConfigStatus).build();
  }
}
