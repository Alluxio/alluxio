/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.security.authentication;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslServer;

/**
 * Tests the {@link PlainSaslServerProvider} class.
 */
public class PlainSaslServerProviderTest {

  /**
   * Tests the {@link Sasl#createSaslServer(String, String, String, Map, CallbackHandler)} method to
   * work with the {@link PlainSaslServerProvider#MECHANISM} successfully.
   */
  @Test
  public void createPlainSaslServerTest() throws Exception {
    // create plainSaslServer
    SaslServer server = Sasl.createSaslServer(PlainSaslServerProvider.MECHANISM, "", "",
        new HashMap<String, String>(), null);
    Assert.assertEquals(PlainSaslServerProvider.MECHANISM, server.getMechanismName());
  }

  /**
   * Tests the {@link Sasl#createSaslServer(String, String, String, Map, CallbackHandler)} method to
   * be null when the provider is not plain.
   */
  @Test
  public void createNoSupportSaslServerTest() throws Exception {
    // create a SaslServer which PlainSaslServerProvider has not supported
    SaslServer server = Sasl.createSaslServer("NO_PLAIN", "", "",
        new HashMap<String, String>(), null);
    Assert.assertNull(server);
  }

  /**
   * Tests the {@link PlainSaslUtils#isPlainSaslProviderAdded()} method.
   */
  @Test
  public void plainSaslProviderHasRegisteredTest() {
    Assert.assertTrue(PlainSaslUtils.isPlainSaslProviderAdded());
  }
}
