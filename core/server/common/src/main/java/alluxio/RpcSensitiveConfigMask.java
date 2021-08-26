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

import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.MapField;
import org.slf4j.Logger;

import java.lang.reflect.Field;
import java.util.Map.Entry;
import java.lang.reflect.Modifier;
import java.util.Map;

/**
 * RpcSensitiveConfigMask is going to mask the credential in properties.
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

  @Override
  public String[] maskAndToString(Logger logger, Object... args) {
    /**
     * Using a generic way to mask any credential in MapField in protobuf generated code.
     * As long as the credential is added in MapField, no necessary to change the code.
     * No extra change is required if a new proto message referring to
     * existing credential holder is added
     */
    String [] strings = new String[args.length];
    for (int i = 0; i < args.length; i++) {
      StringBuilder strBuilder = new StringBuilder();

      if (args[i] instanceof com.google.protobuf.GeneratedMessageV3) {
        traverseAndMask(logger, (GeneratedMessageV3) args[i], strBuilder);
        strings[i] = strBuilder.toString();
      } else {
        strings[i] = strBuilder.append(args[i]).toString();
      }
    }
    return strings;
  }

  /**
   * Traverse the object, and filter credential for FieldMap.
   * @param logger logger writer
   * @param generateMessageV3 object
   * @param strBuilder string builder
   */
  protected void traverseAndMask(Logger logger, GeneratedMessageV3 generateMessageV3,
                              StringBuilder strBuilder) {
    Field[] fields = generateMessageV3.getClass().getDeclaredFields();
    for (Field field : fields) {
      if (Modifier.isStatic(field.getModifiers())) {
        continue;
      }

      try {
        field.setAccessible(true);
        Object obj = field.get(generateMessageV3);
        if (obj == null) {
          continue;
        } else if (obj instanceof GeneratedMessageV3) {
          traverseAndMask(logger, (GeneratedMessageV3) obj, strBuilder);
        } else if (obj instanceof MapField) {
          try {
            MapField<String, String> t = (MapField<String, String>) obj;
            strBuilder.append("properties{\n");
            boolean writeObject = false;
            for (Map.Entry<String, String> entry : t.getMap().entrySet()) {
              if (entry.getKey() instanceof String && entry.getValue() instanceof String) {
                if (!CredentialPropertyKeys.getCredentials().contains(entry.getKey())) {
                  strBuilder.append("key:\"").append(entry.getKey()).append("\"\nvalue:\"")
                      .append(entry.getValue()).append(" \"\n");
                } else {
                  strBuilder.append("key:\"").append(entry.getKey())
                      .append("\"\nvalue:\"Masked\"\n");
                }
              } else {
                writeObject = true;
                break;
              }
            }

            if (writeObject) {
              strBuilder.append(((MapField<?, ?>) obj).getMap()).append("\n");
            }
            strBuilder.append("}\n");
          } catch (ClassCastException e) {
            // in case of cast failure, just print it.
            strBuilder.append(obj).append("\n");
          }
        } else {
          if (field.getType().isPrimitive() || field.getType().isEnum() || obj instanceof String) {
            strBuilder.append(field.getName()).append(":").append(obj.toString()).append("\n");
          } else {
            strBuilder.append(field.getName()).append(":{\n")
                .append(obj.toString()).append("\n}\n");
          }
        }
      } catch (IllegalAccessException e) {
        if (logger != null) {
          logger.error("IllegalAccessException:{} for object:{}",
              e.toString(), generateMessageV3.toString());
        }
      }
    }
  }
}
