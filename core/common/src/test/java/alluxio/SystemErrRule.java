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

import javax.annotation.concurrent.NotThreadSafe;

import java.io.OutputStream;

/**
 * A rule for setting output stream during a test suite. It sets
 * System.err to the specified output stream during the lifetime
 * of this rule.
 */
@NotThreadSafe
public final class SystemErrRule extends AbstractResourceRule {
  private RedirectOutputRule mRedirectOutputRule;

  public SystemErrRule(OutputStream outputStream) {
    mRedirectOutputRule = new RedirectOutputRule(System.err, outputStream, System::setErr);
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
