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

  public void traverseAndMask(Logger logger, GeneratedMessageV3 gmV3, StringBuilder strBuilder) {
    Field[] fields = gmV3.getClass().getDeclaredFields();
    for(Field f : fields) {
      if (Modifier.isStatic(f.getModifiers())) {
        continue;
      }

      try {
        Object obj = f.get(gmV3);
        if (obj instanceof GeneratedMessageV3) {
          traverseAndMask(logger, (GeneratedMessageV3) obj, strBuilder);
        } else if (obj instanceof com.google.protobuf.MapField<java.lang.String, java.lang.String>) {
          strBuilder.append("properties{\n");
          com.google.protobuf.MapField<java.lang.String, java.lang.String> t =
              (com.google.protobuf.MapField<java.lang.String, java.lang.String>) obj;
          for ( String key : t.getMap().keySet()) {
            if (!CredentialConfigItems.CREDENTIALS.contains(key)) {
              strBuilder.append("key:\"").append(key).append("\"\nvalue:\"")
                  .append(t.getMap().get(key)).append("\"\n");
            } else {
              strBuilder.append("key:\"").append(key).append("\"\nvalue:\"Masked\"\n");
            }
          }
          strBuilder.append("\n");
        } else {
          strBuilder.append(obj.toString());
        }
      } catch (IllegalAccessException e) {
        logger.error("IllegalAccessException:{} for object:{}", e.toString(), gmV3.toString());
      }
    }
  }
}
