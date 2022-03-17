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

package alluxio.proxy.s3.signature;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.proxy.s3.S3Exception;
import alluxio.util.CommonUtils;
import alluxio.proxy.s3.S3ErrorCode;

/**
 * Interface to manager s3 secret.
 */
public interface S3SecretManager {

  /**
   * Factory to create {@link S3SecretManager}.
   */
  class Factory {
    private Factory() {} // prevent instantiation

    /**
     * Creates and initializes {@link S3SecretManager} implementation
     * based on Alluxio configuration.
     *
     * @return the generated {@link S3SecretManager} instance
     */
    public static S3SecretManager create(AlluxioConfiguration conf) throws S3Exception {
      Class<?> customManagerClass;
      if (conf.isSet(PropertyKey.S3_SECRET_MANAGER_CLASSNAME)) {
        String customManagerName = conf.get(PropertyKey.S3_SECRET_MANAGER_CLASSNAME);
        try {
          customManagerClass = Class.forName(customManagerName);
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(customManagerName + " not found");
        }

        S3SecretManager secretManager =  (S3SecretManager) CommonUtils
                .createNewClassInstance(customManagerClass, null, null);
        secretManager.init(conf);
        return secretManager;
      } else {
        throw new S3Exception("No default secret manager",
                S3ErrorCode.NOT_FOUND_CUSTOMIZED_SECRET_MANAGER);
      }
    }
  }

  /**
   * init method.
   * @param conf alluxio conf
   */
  void init(AlluxioConfiguration conf);

  /**
   * API to get s3 secret for given awsAccessKey.
   * @param awsAccessKey
   * @return secret
   * */
  String getS3UserSecretString(String awsAccessKey) throws S3Exception;
}
