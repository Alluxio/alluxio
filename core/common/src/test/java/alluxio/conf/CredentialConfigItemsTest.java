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

package alluxio.conf;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import java.util.Set;

/**
 * Unit tests for {@link CredentialPropertyKeys}.
 */
public class CredentialConfigItemsTest {
  @Test
  public void getCredentials() {
    Set<?> credentialSet = (Set<?>) CredentialPropertyKeys.getCredentials();
    assertEquals(true, credentialSet.contains("aws.accessKeyId"));
    assertEquals(false, credentialSet.contains("aws.accessKeyId11"));
  }

  @Test
  public void testException() {
    Set<?> credentialSet = (Set<?>) CredentialPropertyKeys
        .findCredentialPropertyKeys("not_exist_class");
    assertEquals(0, credentialSet.size());

    credentialSet = (Set<?>) CredentialPropertyKeys
        .findCredentialPropertyKeys("CredentialConfigItemsTest.MockPropertyKey");
    assertEquals(0, credentialSet.size());
  }
}
