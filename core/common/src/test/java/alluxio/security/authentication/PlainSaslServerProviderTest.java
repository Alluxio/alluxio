/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.security.authentication;

import java.util.HashMap;
import java.util.Map;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslServer;

import org.junit.Assert;
import org.junit.Test;

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
