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
import java.util.Map;

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
        if (obj == null) {
        } else if (obj instanceof GeneratedMessageV3) {
          traverseAndMask(logger, (GeneratedMessageV3) obj, strBuilder);
        } else if (obj instanceof MapField) {
          try {
            MapField<String, String> t = (MapField<String, String>) obj;
            strBuilder.append("properties{\n");
            boolean writeObject = false;
            for (Map.Entry<String,String> entry : t.getMap().entrySet()) {
              if (entry.getKey() instanceof String && entry.getValue() instanceof String)
              {
                if (!CredentialConfigItems.getCredentials().contains(entry.getKey())) {
                  strBuilder.append("key:\"").append(entry.getKey() ).append("\"\nvalue:\"")
                      .append(entry.getValue()).append(" \"\n");
                } else {
                  logger.debug("find credential {}", entry.getKey() );
                  strBuilder.append("key:\"").append(entry.getKey()).append("\"\nvalue:\"Masked\"\n");
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
            strBuilder.append(field.getName()).append(":{\n").append(obj.toString()).append("\n}\n");
          }
        }
      } catch (IllegalAccessException e) {
        logger.error("IllegalAccessException:{} for object:{}", e.toString(), generateMessageV3.toString());
      }
    }
  }
}