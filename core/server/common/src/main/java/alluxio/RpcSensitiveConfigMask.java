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
import alluxio.conf.CredentialConfigItems;

import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.MapField;
import org.slf4j.Logger;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * RpcSensitiveConfigMask is going to mask the credential in properties.
 */
public class RpcSensitiveConfigMask implements SensitiveConfigMask {
  public static final RpcSensitiveConfigMask RPCSENSITIVECMASK;

  static {
    RPCSENSITIVECMASK = new RpcSensitiveConfigMask();
    activeMask();
  }

  /**
   * Active rpc mask.
   */
  public static void activeMask() {
    RpcUtils.SMASK = RPCSENSITIVECMASK;
  }

  @Override
  public String maskAndToString(Logger logger, Object... args) {
    StringBuilder strBuilder = new StringBuilder();
    for (Object obj : args) {
      if (obj instanceof com.google.protobuf.GeneratedMessageV3) {
        traverseAndMask(logger, (GeneratedMessageV3) obj, strBuilder);
      } else {
        strBuilder.append(obj);
      }
    }
    return strBuilder.toString();
  }

  /**
   * Traverse the object, and filter credential for FieldMap.
   * @param logger logger writer
   * @param generateMessageV3 object
   * @param strBuilder string builder
   */
  public void traverseAndMask(Logger logger, GeneratedMessageV3 generateMessageV3,
                              StringBuilder strBuilder) {
    Field[] fields = generateMessageV3.getClass().getDeclaredFields();
    for(Field field : fields) {
      if (Modifier.isStatic(field.getModifiers())) {
        continue;
      }

      try {
        field.setAccessible(true);
        Object obj = field.get(generateMessageV3);
        if (obj instanceof GeneratedMessageV3) {
          traverseAndMask(logger, (GeneratedMessageV3) obj, strBuilder);
        } else if (obj instanceof MapField) {
          try {
            MapField<String, String> t = (MapField<String, String>) obj;
            strBuilder.append("properties{\n");
            for (String key : t.getMap().keySet()) {
              if (!CredentialConfigItems.CREDENTIALS.contains(key)) {
                strBuilder.append("key:\"").append(key).append("\"\nvalue:\"")
                    .append(t.getMap().get(key)).append(" \"\n");
              } else {
                strBuilder.append("key:\"").append(key).append("\"\nvalue:\"Masked\"\n");
              }
            }
            strBuilder.append("\n");
          } catch (ClassCastException e) {
            logger.error("Error found during log generating, exception:{}, object:{}",
                e.toString(), generateMessageV3.toString());
            strBuilder.append(obj.toString()).append("\n");
          }
        } else {
          strBuilder.append(field.getName()).append(":").append(obj.toString()).append("\n");
        }
      } catch (IllegalAccessException e) {
        logger.error("IllegalAccessException:{} for object:{}", e.toString(), generateMessageV3.toString());
      }
    }
  }
}