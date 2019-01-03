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

package alluxio.security.authentication.plain;

import java.security.Provider;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The Java SunSASL provider supports CRAM-MD5, DIGEST-MD5 and GSSAPI mechanisms on the server side.
 * When the SASL is using PLAIN mechanism, there is no support the SASL server. So there is a new
 * provider needed to register to support server-side PLAIN mechanism.
 * <p/>
 * Three basic steps to complete a SASL security provider:
 * <ol>
 * <li>Implements {@link PlainSaslServer} class which extends {@link javax.security.sasl.SaslServer}
 * interface</li>
 * <li>Provides {@link PlainSaslServer.Factory} class that implements
 * {@link javax.security.sasl.SaslServerFactory} interface</li>
 * <li>Provides a JCA provider that registers the factory</li>
 * </ol>
 */
@ThreadSafe
public final class PlainSaslServerProvider extends Provider {
  private static final long serialVersionUID = 4583558117355348638L;

  public static final String NAME = "PlainSasl";
  public static final String MECHANISM = "PLAIN";
  public static final double VERSION = 1.0;

  /**
   * Constructs a new provider for the SASL server when using the PLAIN mechanism.
   */
  public PlainSaslServerProvider() {
    super(NAME, VERSION, "Plain SASL server provider");
    put("SaslServerFactory." + MECHANISM, PlainSaslServer.Factory.class.getName());
  }
}
