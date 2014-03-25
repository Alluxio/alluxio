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
package tachyon.master;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;

import tachyon.Constants;

public class RecomputeCommand implements Runnable {
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private final String CMD;
  private final String FILE_PATH;

  public RecomputeCommand(String cmd, String filePath) {
    CMD = cmd;
    FILE_PATH = filePath;
  }

  @Override
  public void run() {
    try {
      LOG.info("Exec " + CMD + " output to " + FILE_PATH);
      Process p = java.lang.Runtime.getRuntime().exec(CMD);
      String line;
      BufferedReader bri = new BufferedReader(new InputStreamReader(p.getInputStream()));
      BufferedReader bre = new BufferedReader(new InputStreamReader(p.getErrorStream()));
      File file = new File(FILE_PATH);
      FileWriter fw = new FileWriter(file.getAbsoluteFile());
      BufferedWriter bw = new BufferedWriter(fw);
      while ((line = bri.readLine()) != null) {
        bw.write(line + "\n");
      }
      bri.close();
      while ((line = bre.readLine()) != null) {
        bw.write(line + "\n");
      }
      bre.close();
      bw.flush();
      bw.close();
      p.waitFor();
      LOG.info("Exec " + CMD + " output to " + FILE_PATH + " done.");
    } catch (IOException e) {
      LOG.error(e.getMessage());
    } catch (InterruptedException e) {
      LOG.error(e.getMessage());
    }
  }
}
