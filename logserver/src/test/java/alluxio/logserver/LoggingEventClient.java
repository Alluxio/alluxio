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

package alluxio.logserver;

import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingEventClient {

  static Logger LOG = LoggerFactory.getLogger(LoggingEventClient.class);

  private static void setProperties() {
    Properties properties = new Properties();
    properties.setProperty("log4j.rootLogger", "ALL,socket");
    properties.setProperty("log4j.appender.socket", "org.apache.log4j.net.SocketAppender");
    properties.setProperty("log4j.appender.socket.RemoteHost", "localhost");
    properties.setProperty("log4j.appender.socket.port", "5002");
    properties.setProperty("log4j.appender.socket.application", "localclient");
    PropertyConfigurator.configure(properties);
  }

  public static void main(String[] args) {
    setProperties();

    int intNum = 1;
    double doubleNum = 1.0;
    String string = "string";

    //The number of log statements should be consistent with the variable values in the TestServer class
    LOG.debug("this is int type: {}", intNum);
    LOG.info("this is double type: {}", doubleNum);
    LOG.error("this is string type: {}", string);
  }
}
