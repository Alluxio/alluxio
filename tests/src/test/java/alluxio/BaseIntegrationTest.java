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

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/**
 * Base class used for specifying the maximum time a test should run for.
 */
public abstract class BaseIntegrationTest {

  @Rule
  public Timeout mGlobalTimeout = Timeout.millis(Constants.MAX_TEST_DURATION_MS);

  @Rule
  public TestWatcher mWatcher = new TestWatcher() {
    // When tests fail, save the logs.
    protected void failed(Throwable t, Description description) {
      try {
        Files.copy(Paths.get(Constants.TESTS_LOG),
            Paths.get(Constants.TEST_LOG_DIR, String.format("%s-%s.log",
                description.getClassName(), description.getMethodName())),
            StandardCopyOption.REPLACE_EXISTING);
      } catch (IOException e) {
        if (t != null) {
          t.addSuppressed(e);
        } else {
          throw new RuntimeException(e);
        }
      }
      return;
    }

    // Before each test starts, truncate the log file.
    protected void starting(Description description) {
      try {
        new FileWriter(Constants.TESTS_LOG).close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  };
}
