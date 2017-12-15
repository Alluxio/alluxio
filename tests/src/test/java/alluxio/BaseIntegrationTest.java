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

import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.rules.Timeout;
import org.junit.runner.Description;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/**
 * Base class used for specifying the maximum time a test should run for.
 */
public abstract class BaseIntegrationTest {
  private static final File LOG_DIR = new File("./target/logs");
  private static final File TESTS_LOG = new File("./target/logs/tests.log");

  @Rule
  public Timeout mGlobalTimeout = Timeout.millis(Constants.MAX_TEST_DURATION_MS);

  @Rule
  public TestWatcher mWatcher = new TestWatcher() {
    // When tests fail, save the logs.
    protected void failed(Throwable e, Description description) {
      try {
        Files.copy(Paths.get(TESTS_LOG.toURI()),
            Paths.get(LOG_DIR.getAbsolutePath(), String.format("%s-%s.log",
                description.getClassName(), description.getMethodName())),
            StandardCopyOption.REPLACE_EXISTING);
      } catch (IOException e1) {
        if (e != null) {
          e.addSuppressed(e1);
        } else {
          throw new RuntimeException(e1);
        }
      }
      return;
    }

    protected void succeeded(Description description) {
      // When tests succeed, reset the log file.
      try {
        Files.write(Paths.get(TESTS_LOG.getAbsolutePath()), "".getBytes());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  };
}
