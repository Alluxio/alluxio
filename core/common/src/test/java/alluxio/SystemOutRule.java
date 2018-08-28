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

import java.io.OutputStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A rule for setting output stream during a test suite. It sets
 * System.out to the specified output stream during the lifetime
 * of this rule.
 */
@NotThreadSafe
public final class SystemOutRule extends AbstractResourceRule {
  private RedirectOutputRule mRedirectOutputRule;

  /**
   * @param outputStream the output stream to set as output
   */
  public SystemOutRule(OutputStream outputStream) {
    mRedirectOutputRule = new RedirectOutputRule(System.out, outputStream, System::setOut);
  }

  @Override
  protected void before() throws Exception {
    mRedirectOutputRule.before();
  }

  @Override
  protected void after() {
    mRedirectOutputRule.after();
  }
}
