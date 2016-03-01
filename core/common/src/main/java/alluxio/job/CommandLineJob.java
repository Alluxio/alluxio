/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.job;

import alluxio.Constants;
import alluxio.wire.CommandLineJobInfo;
import alluxio.wire.JobConfInfo;

import com.google.common.base.Objects;
import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A job that wraps a programmed run by command line. This job's caller should ensure the execution
 * environment are identical on master and at the client side.
 */
@ThreadSafe
public class CommandLineJob extends Job {
  private static final long serialVersionUID = 1655996721855899996L;
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final String mCommand;

  /**
   * Creates a new instance of {@link CommandLineJob}.
   *
   * @param command the command that can run in shell
   * @param jobConf the job configuration
   */
  public CommandLineJob(String command, JobConf jobConf) {
    super(jobConf);
    mCommand = command;
  }

  /**
   * Generates the {@link CommandLineJobInfo} for the command.
   *
   * @return the {@link CommandLineJobInfo} for RPC
   */
  public CommandLineJobInfo generateCommandLineJobInfo() {
    return new CommandLineJobInfo().setCommand(mCommand).setConf(
        new JobConfInfo().setOutputFile(getJobConf().getOutputFilePath()));
  }

  /**
   * Gets the command.
   *
   * @return the command
   */
  public String getCommand() {
    return mCommand;
  }

  @Override
  public boolean run() {
    try {
      String outputPath = getJobConf().getOutputFilePath();
      LOG.info("Exec {} output to {}", mCommand, outputPath);
      Process p = java.lang.Runtime.getRuntime().exec(mCommand);
      String line;
      Closer closer = Closer.create();
      try {
        BufferedReader bri =
            closer.register(new BufferedReader(new InputStreamReader(p.getInputStream())));
        BufferedReader bre =
            closer.register(new BufferedReader(new InputStreamReader(p.getErrorStream())));
        File file = new File(outputPath);
        FileWriter fw = new FileWriter(file.getAbsoluteFile());
        BufferedWriter bw = closer.register(new BufferedWriter(fw));
        while ((line = bri.readLine()) != null) {
          bw.write(line + "\n");
        }
        while ((line = bre.readLine()) != null) {
          bw.write(line + "\n");
        }
        bw.flush();
      } finally {
        closer.close();
      }

      p.waitFor();
      LOG.info("Exec {} output to {} done.", mCommand, outputPath);
    } catch (IOException e) {
      LOG.error(e.getMessage());
      return false;
    } catch (InterruptedException e) {
      LOG.error(e.getMessage());
      return false;
    }

    return true;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("command", mCommand).toString();
  }
}
