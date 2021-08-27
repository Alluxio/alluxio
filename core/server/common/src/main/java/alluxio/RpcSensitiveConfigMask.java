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

import alluxio.conf.SensitiveConfigMask;
import alluxio.conf.CredentialPropertyKeys;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.MountPRequest;
import alluxio.grpc.UfsInfo;
import alluxio.grpc.GetUfsInfoPResponse;
import alluxio.grpc.UpdateMountPRequest;

import org.slf4j.Logger;

import java.util.Map.Entry;
import java.util.Map;

/**
 * RpcSensitiveConfigMask is going to mask the credential RPC messages.
 */
public class RpcSensitiveConfigMask implements SensitiveConfigMask {
  public static final RpcSensitiveConfigMask CREDENTIAL_FIELD_MASKER;

  static {
    CREDENTIAL_FIELD_MASKER = new RpcSensitiveConfigMask();
  }

  @Override
  public Object[] maskObjects(Logger logger, Object... args) {
    /**
     * This function is to mask MountPOption, and those who are referring to it.
     * If something else need be masked, extra code change is required.
     * And also if a new proto message referring direct/indirect to MountPOption,
     * extra code should be added here.
     */
    Object [] objects = new Object[args.length];
    for (int i = 0; i < args.length; i++) {
      if (args[i] instanceof MountPOptions) {
        MountPOptions.Builder newMP = MountPOptions.newBuilder((MountPOptions) args[i]);
        newMP.clearProperties();
        copyAndMaskProperties(newMP, ((MountPOptions) args[i]).getPropertiesMap());
        objects[i] = newMP.build();
      } else if (args[i] instanceof MountPRequest) {
        MountPRequest.Builder mpR = MountPRequest.newBuilder((MountPRequest) args[i]);
        MountPOptions.Builder newMP = mpR.getOptionsBuilder();
        newMP.clearProperties();
        copyAndMaskProperties(newMP, ((MountPRequest) args[i]).getOptions().getPropertiesMap());
        objects[i] = mpR.build();
      } else if (args[i] instanceof UfsInfo) {
        UfsInfo.Builder ufsInfo = UfsInfo.newBuilder((UfsInfo) args[i]);
        MountPOptions.Builder newMP = ufsInfo.getPropertiesBuilder();
        newMP.clearProperties();
        copyAndMaskProperties(newMP, ((UfsInfo) args[i]).getProperties().getPropertiesMap());
        objects[i] = ufsInfo.build();
      } else if (args[i] instanceof GetUfsInfoPResponse) {
        GetUfsInfoPResponse.Builder getUfsInfoResponse =
            GetUfsInfoPResponse.newBuilder((GetUfsInfoPResponse) args[i]);
        MountPOptions.Builder newMP = getUfsInfoResponse.getUfsInfoBuilder().getPropertiesBuilder();
        newMP.clearProperties();
        copyAndMaskProperties(newMP,
            ((GetUfsInfoPResponse) args[i]).getUfsInfo().getProperties().getPropertiesMap());
        objects[i] = getUfsInfoResponse.build();
      } else if (args[i] instanceof UpdateMountPRequest) {
        UpdateMountPRequest.Builder updateMountPRequest =
            UpdateMountPRequest.newBuilder((UpdateMountPRequest) args[i]);
        MountPOptions.Builder newMP = updateMountPRequest.getOptionsBuilder();
        newMP.clearProperties();
        copyAndMaskProperties(newMP, ((UpdateMountPRequest) args[i])
            .getOptions().getPropertiesMap());
        objects[i] = updateMountPRequest.build();
      } else {
        objects[i] = args[i];
      }
    }

    return objects;
  }

  protected void copyAndMaskProperties(MountPOptions.Builder builder, Map<String, String> rawMap) {
    for (Entry entry : rawMap.entrySet()) {
      if (!CredentialPropertyKeys.getCredentials().contains(entry.getKey())) {
        builder.putProperties((String) entry.getKey(), (String) entry.getValue());
      } else {
        builder.putProperties((String) entry.getKey(), "Masked");
      }
    }
  }
}
