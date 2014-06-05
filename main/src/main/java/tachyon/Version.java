/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon;

import java.io.InputStream;
import java.io.IOException;
import java.util.Properties;
import org.apache.log4j.Logger;

/**
 * The version of the current build.
 */
public class Version {
  public static final String VERSION;

  private static Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  static {
    InputStream in = null;
    Properties p = new Properties();

    try {
      in = Version.class.getClassLoader().getResourceAsStream("version.properties");
      p.load(in);
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
    } finally {
      try {
        in.close();
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
      }
    }

    VERSION = p.getProperty("tachyon.version", "UNDEFINED");
  }

  public static void main(String[] args) {
    System.out.println("Tachyon version: " + VERSION);
  }
}
