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

package alluxio.shell.command;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.MountOptions;
import alluxio.exception.AlluxioException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Mounts a UFS path onto an Alluxio path.
 */
@ThreadSafe
public final class MountCommand extends AbstractShellCommand {

  /**
   * @param conf the configuration for Alluxio
   * @param fs the filesystem of Alluxio
   */
  public MountCommand(Configuration conf, FileSystem fs) {
    super(conf, fs);
  }

  @Override
  public String getCommandName() {
    return "mount";
  }

  @Override
  protected int getNumOfArgs() {
    return 2;
  }

  @Override
  protected Options getOptions() {
    return new Options().addOption(PROPERTY_FILE_OPTION).addOption(READONLY_OPTION);
  }

  @Override
  public void run(CommandLine cl) throws IOException {
    String[] args = cl.getArgs();
    AlluxioURI alluxioPath = new AlluxioURI(args[0]);
    AlluxioURI ufsPath = new AlluxioURI(args[1]);
    MountOptions options = MountOptions.defaults();

    String propertyFile = cl.getOptionValue('P');
    if (propertyFile != null) {
      Properties cmdProps = new Properties();
      try (InputStream inStream = new FileInputStream(propertyFile)) {
        cmdProps.load(inStream);
      } catch (IOException e) {
        throw new IOException("Unable to load property file: " + propertyFile);
      }

      if (!cmdProps.isEmpty()) {
        // Use the properties from the properties file for the mount options.
        Map<String, String> properties = new HashMap<>();
        for (Map.Entry<Object, Object> entry : cmdProps.entrySet()) {
          properties.put(entry.getKey().toString(), entry.getValue().toString());
        }
        options.setProperties(properties);
        System.out.println("Using properties from file: " + propertyFile);
      }
    }

    if (cl.hasOption("readonly")) {
      options.setReadOnly(true);
    }

    try {
      mFileSystem.mount(alluxioPath, ufsPath, options);
      System.out.println("Mounted " + ufsPath + " at " + alluxioPath);
    } catch (AlluxioException e) {
      throw new IOException(e.getMessage());
    }
  }

  @Override
  public String getUsage() {
    return "mount [-readonly] [-P <properties file name>] <alluxioPath> <ufsURI>";
  }

  @Override
  public String getDescription() {
    return "Mounts a UFS path onto an Alluxio path.";
  }
}
