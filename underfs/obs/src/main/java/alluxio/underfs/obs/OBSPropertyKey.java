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

import alluxio.conf.PropertyKey;

import javax.annotation.concurrent.ThreadSafe;

/**
 * OBS configuration property keys.
 */
@ThreadSafe
public final class OBSPropertyKey {
  public static final PropertyKey OBS_ACCESS_KEY = new PropertyKey.Builder(Name.OBS_ACCESS_KEY)
      .setDescription("The access key of OBS bucket.").build();
  public static final PropertyKey OBS_ENDPOINT = new PropertyKey.Builder(Name.OBS_ENDPOINT)
      .setDefaultValue("obs.myhwclouds.com")
      .setDescription("The endpoint of OBS bucket.").build();
  public static final PropertyKey OBS_SECRET_KEY = new PropertyKey.Builder(Name.OBS_SECRET_KEY)
      .setDescription("The secret key of OBS bucket.").build();
  public static final PropertyKey OBS_BUCKET_TYPE = new PropertyKey.Builder(Name.OBS_BUCKET_TYPE)
          .setDefaultValue("obs")
          .setDescription("The type of bucket (obs/pfs),the default value is obs.").build();
  /**
   * Name for OBS configuration property keys.
   */
  @ThreadSafe
  public static final class Name {
    public static final String OBS_ACCESS_KEY = "fs.obs.accessKey";
    public static final String OBS_ENDPOINT = "fs.obs.endpoint";
    public static final String OBS_SECRET_KEY = "fs.obs.secretKey";
    public static final String OBS_BUCKET_TYPE = "fs.obs.bucketType";
  }
}
