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

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import javax.annotation.concurrent.GuardedBy;

public class TestLoggerRule extends AbstractResourceRule {

  private TestAppender mAppender;

  @Before
  public void before() {
    mAppender = new TestAppender();
    Logger.getRootLogger().addAppender(mAppender);
  }

  @After
  public void after() {
    Logger.getRootLogger().removeAppender(mAppender);
  }

  /**
   * Determine if a specific pattern appears in log output.
   *
   * @param pattern a pattern text to search for in log events
   * @return true if a log message containing the pattern exists, false otherwise
   */
  public boolean wasLogged(String pattern) {
    return mAppender.wasLogged(Pattern.compile(".*" + pattern + ".*"));
  }

  public class TestAppender extends AppenderSkeleton {
    @GuardedBy("this")
    private List<LoggingEvent> mEvents = new ArrayList<>();

    public void close() { }

    /**
     * Determines whether a message with the given pattern was logged.
     */
    public synchronized boolean wasLogged(Pattern pattern) {
      for (LoggingEvent e : mEvents) {
        if (pattern.matcher(e.getRenderedMessage()).matches()) {
          return true;
        }
      }
      return false;
    }

    @Override
    public boolean requiresLayout() {
      return false;
    }

    @Override
    protected synchronized void append(LoggingEvent event) {
      mEvents.add(event);
    }
  }

}

