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

package alluxio.underfs.obs;

import com.obs.services.ObsClient;

import java.util.Map;

public class ObsClientExt extends ObsClient {
  public ObsClientExt(String accessKey, String secretKey, String endPoint,
      Map<String, Object> conf) {
    super(accessKey, secretKey, endPoint);
    for (Map.Entry<String, Object> entry : conf.entrySet()) {
      obsProperties.setProperty(entry.getKey(),
          entry.getValue() == null ? null : entry.getValue().toString());
    }
  }
}
