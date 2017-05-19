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

package alluxio.underfs.swift;

import org.javaswift.joss.client.factory.TempUrlHashPrefixSource;
import org.javaswift.joss.model.Access;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom {@link Access} for Keystone V3 authentication.
 */
public class KeystoneV3Access implements Access {
  private static final Logger LOG = LoggerFactory.getLogger(KeystoneV3Access.class);

  private String mInternalURL;
  private String mPrefferedRegion;
  private String mPublicURL;
  private String mToken;

  /**
   * Construct a new instance of {@link KeystoneV3Access}.
   *
   * @param internalURL internal object endpoint URL
   * @param preferredRegion preferred region for object store
   * @param publicURL public object endpoint URL
   * @param token access token
   */
  public KeystoneV3Access(String internalURL, String preferredRegion, String publicURL,
      String token) {
    mInternalURL = internalURL;
    mPrefferedRegion = preferredRegion;
    mPublicURL = publicURL;
    mToken = token;
  }

  @Override
  public String getInternalURL() {
    return mInternalURL;
  }

  @Override
  public String getPublicURL() {
    return mPublicURL;
  }

  @Override
  public String getTempUrlPrefix(TempUrlHashPrefixSource arg0) {
    return null;
  }

  @Override
  public String getToken() {
    return mToken;
  }

  @Override
  public boolean isTenantSupplied() {
    return true;
  }

  @Override
  public void setPreferredRegion(String region) {
    mPrefferedRegion = region;
    LOG.debug("Preferred region is set to {}", mPrefferedRegion);
  }
}
