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

package alluxio.util.logging;

import org.apache.log4j.Appender;
import org.apache.log4j.Layout;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;

import java.io.StringWriter;

/**
 * A common tool for capturing log information.
 */
public final class LogCapturer {
  private StringWriter mStringWriter = new StringWriter();
  private WriterAppender mWriterAppender;
  private Logger mLogger;

  public static LogCapturer captureLogs(org.slf4j.Logger logger) {
    if (logger.getName().equals("root")) {
      return new LogCapturer(org.apache.log4j.Logger.getRootLogger());
    }
    return new LogCapturer(toLog4j(logger));
  }

  public static LogCapturer captureLogs(Logger logger) {
    return new LogCapturer(logger);
  }

  private LogCapturer(Logger logger) {
    mLogger = logger;
    Appender defaultAppender = Logger.getRootLogger().getAppender("stdout");
    if (defaultAppender == null) {
      defaultAppender = Logger.getRootLogger().getAppender("console");
    }
    Layout layout = (defaultAppender == null) ? new PatternLayout() : defaultAppender.getLayout();
    mWriterAppender = new WriterAppender(layout, this.mStringWriter);
    logger.addAppender(this.mWriterAppender);
  }

  public static Logger toLog4j(org.slf4j.Logger logger) {
    return LogManager.getLogger(logger.getName());
  }

  public String getOutput() {
    return this.mStringWriter.toString();
  }

  public void stopCapturing() {
    mLogger.removeAppender(mWriterAppender);
  }

  public void clearOutput() {
    mStringWriter.getBuffer().setLength(0);
  }
}
