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
   * Determine if a specific piece of text appears in log output.
   *
   * @param eventString The piece of text to search for in log events
   * @return True if an event containing the string exists, false otherwise
   */
  public boolean wasLogged(String eventString) {
    return mAppender.wasLogged(eventString);
  }

  public class TestAppender extends AppenderSkeleton {

    public List<LoggingEvent> mEvents = new ArrayList<LoggingEvent>();

    public void close() { }

    /**
     * Determines whether string was logged.
     */
    public boolean wasLogged(String eventString) {
      for (LoggingEvent e : mEvents) {
        if (e.getRenderedMessage().contains(eventString)) {
          return true;
        }
      }
      return false;
    }

    public boolean requiresLayout() {
      return false;
    }

    @Override
    protected void append(LoggingEvent event) {
      mEvents.add(event);
    }
  }

}

