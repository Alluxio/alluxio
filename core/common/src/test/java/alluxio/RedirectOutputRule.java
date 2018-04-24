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
import java.io.PrintStream;
import java.util.function.Consumer;

/**
 * A rule for redirecting an output stream during a test suite. It sets
 * a print stream to the specified output stream during the lifetime
 * of this rule.
 */
@NotThreadSafe
public final class RedirectOutputRule extends AbstractResourceRule {
  private final OutputStream mOutputStream;
  private final Consumer<PrintStream> mRedirector;
  private PrintStream mInputStream;
  private PrintStream mNewOutput;
  private PrintStream mOldOutput;

  /**
   * @param inputStream the print stream to set as input
   * @param outputStream the output stream to set as output
   * @param redirector the stream redirection function (e.g. System::setOut)
   */
  public RedirectOutputRule(PrintStream inputStream, OutputStream outputStream,
                            Consumer<PrintStream> redirector) {
    mInputStream = inputStream;
    mOutputStream = outputStream;
    mRedirector = redirector;
  }

  @Override
  protected void before() throws Exception {
    mNewOutput = new PrintStream(mOutputStream);
    mOldOutput = mInputStream;
    mRedirector.accept(mNewOutput);
  }

  @Override
  protected void after() {
    mRedirector.accept(mOldOutput);
  }
}
