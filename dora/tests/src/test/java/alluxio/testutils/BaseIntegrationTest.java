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

package alluxio.testutils;

import alluxio.Constants;
import alluxio.util.io.PathUtils;

import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.LogManager;
import org.apache.log4j.PatternLayout;
import org.junit.AssumptionViolatedException;
import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestWatcher;
import org.junit.rules.Timeout;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Base class used for specifying the maximum time a test should run for.
 */
public abstract class BaseIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(BaseIntegrationTest.class);

  @Rule
  public RuleChain mRules = RuleChain.outerRule(logHandler())
      .around(Timeout.millis(Constants.MAX_TEST_DURATION_MS));

  private TestWatcher logHandler() {
    return new TestWatcher() {
      private String mLogPath;
      private Appender mAppender;

      @Override
      protected void starting(Description description) {
        try {
          mLogPath = logPath(description);
          // In case the file already exists, truncate it.
          new FileWriter(mLogPath).close();
          mAppender =
              new FileAppender(new PatternLayout("%d{ISO8601} [%t] %-5p %c{2} (%F:%M) - %m%n"),
                  mLogPath);
          LogManager.getRootLogger().addAppender(mAppender);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      protected void succeeded(Description description) {
        try {
          Files.delete(Paths.get(mLogPath));
        } catch (Throwable t) {
          LOG.error("Failed to delete success log file {}", mLogPath);
        }
      }

      @Override
      protected void skipped(AssumptionViolatedException e, Description description) {
        succeeded(description);
      }

      @Override
      protected void finished(Description description) {
        LogManager.getRootLogger().removeAppender(mAppender);
      }

      private String logPath(Description description) {
        String basename = String.format("tests-%s-%s.log", description.getClassName(),
            description.getMethodName());
        return PathUtils.concatPath(Constants.TEST_LOG_DIR, basename);
      }
    };
  }
}
