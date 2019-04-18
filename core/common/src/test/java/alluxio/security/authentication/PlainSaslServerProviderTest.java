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

package alluxio.security.authentication;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import alluxio.security.authentication.plain.PlainSaslServerProvider;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.security.Security;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslServer;

/**
 * Tests the {@link PlainSaslServerProvider} class.
 */
public final class PlainSaslServerProviderTest {

  @BeforeClass
  public static void beforeClass() {
    Security.addProvider(new PlainSaslServerProvider());
  }

  @AfterClass
  public static void afterClass() {
    Security.removeProvider(PlainSaslServerProvider.NAME);
  }

  /**
   * Tests the {@link Sasl#createSaslServer(String, String, String, Map, CallbackHandler)} method to
   * work with the {@link PlainSaslServerProvider#MECHANISM} successfully.
   */
  @Test
  public void createPlainSaslServer() throws Exception {
    // create plainSaslServer
    SaslServer server = Sasl.createSaslServer(PlainSaslServerProvider.MECHANISM, "", "",
        new HashMap<String, String>(), null);
    assertNotNull(server);
    assertEquals(PlainSaslServerProvider.MECHANISM, server.getMechanismName());
  }

  /**
   * Tests the {@link Sasl#createSaslServer(String, String, String, Map, CallbackHandler)} method to
   * be null when the provider is not plain.
   */
  @Test
  public void createNoSupportSaslServer() throws Exception {
    // create a SaslServer which SecurityProvider has not supported
    SaslServer server = Sasl.createSaslServer("NO_PLAIN", "", "",
        new HashMap<String, String>(), null);
    assertNull(server);
  }
}
