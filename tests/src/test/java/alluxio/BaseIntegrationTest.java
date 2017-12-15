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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

/**
 * Base class used for specifying the maximum time a test should run for.
 */
public abstract class BaseIntegrationTest {

  @Rule
  public Timeout mGlobalTimeout = Timeout.millis(Constants.MAX_TEST_DURATION_MS);

  @Rule
  public TestWatcher mWatcher = new TestWatcher() {
    // When tests fail, save the logs.
    protected void failed(Throwable e, Description description) {
      try {
        Files.copy(Paths.get(Constants.TESTS_LOG),
            Paths.get(Constants.TEST_LOG_DIR, String.format("%s-%s.log",
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

    // Before each test starts, truncate the log file.
    protected void starting(Description description) {
      try {
        Files.write(Paths.get(Constants.TESTS_LOG), new byte[0],
            StandardOpenOption.TRUNCATE_EXISTING);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  };
}
