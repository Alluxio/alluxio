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

package alluxio.cli.fs.command;

import alluxio.AlluxioURI;
import alluxio.cli.Command;
import alluxio.cli.CommandReader;
import alluxio.cli.fs.FileSystemShellUtils;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.util.ConfigurationUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Joiner;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * The base class for all the FileSystem {@link alluxio.cli.Command} classes.
 * It provides a place to hold the {@link FileSystem} client.
 */
@ThreadSafe
public abstract class AbstractFileSystemCommand implements Command {

  protected FileSystem mFileSystem;
  protected FileSystemContext mFsContext;

  private static final ObjectMapper OBJECTMAPPER = new ObjectMapper(new YAMLFactory());
  // The FilesystemContext contains configuration information and is also used to instantiate a
  // filesystem client, if null - load default properties
  protected AbstractFileSystemCommand(@Nullable FileSystemContext fsContext) {
    if (fsContext == null) {
      fsContext =
          FileSystemContext.create(new InstancedConfiguration(ConfigurationUtils.defaults()));
    }
    mFsContext = fsContext;
    mFileSystem = FileSystem.Factory.create(fsContext);
  }

  /**
   * Runs the command for a particular URI that does not contain wildcard in its path.
   *
   * @param plainPath an AlluxioURI that does not contain wildcard
   * @param cl object containing the original commandLine
   */
  protected void runPlainPath(AlluxioURI plainPath, CommandLine cl)
      throws AlluxioException, IOException {
  }

  /**
   * Processes the header of the command. Our input path may contain wildcard
   * but we only want to print the header for once.
   *
   * @param cl object containing the original commandLine
   */
  protected void processHeader(CommandLine cl) throws IOException {
  }

  /**
   * Runs the command for a particular URI that may contain wildcard in its path.
   *
   * @param wildCardPath an AlluxioURI that may or may not contain a wildcard
   * @param cl object containing the original commandLine
   */
  protected void runWildCardCmd(AlluxioURI wildCardPath, CommandLine cl) throws IOException {
    List<AlluxioURI> paths = FileSystemShellUtils.getAlluxioURIs(mFileSystem, wildCardPath);
    if (paths.size() == 0) { // A unified sanity check on the paths
      throw new IOException(wildCardPath + " does not exist.");
    }
    paths.sort(Comparator.comparing(AlluxioURI::getPath));

    // TODO(lu) if errors occur in runPlainPath, we may not want to print header
    processHeader(cl);

    List<String> errorMessages = new ArrayList<>();
    for (AlluxioURI path : paths) {
      try {
        runPlainPath(path, cl);
      } catch (AlluxioException | IOException e) {
        errorMessages.add(e.getMessage() != null ? e.getMessage() : e.toString());
      }
    }

    if (errorMessages.size() != 0) {
      throw new IOException(Joiner.on('\n').join(errorMessages));
    }
  }

  protected URL getCommandFile(Class c) {
    return c.getClassLoader().getResource(String.format("%s.yml", c.getSimpleName()));
  }

  @Override
  public String getCommandName() {
    return getDocs().getName();
  }

  @Override
  public String getUsage() {
    return getDocs().getUsage();
  }

  @Override
  public String getDescription() {
    return getDocs().getDescription();
  }

  @Override
  public String getExample() {
    return getDocs().getExample();
  }

  @Override
  public String getDocumentation() {
    return getDocs().toString();
  }

  private CommandReader getDocs() {
    OBJECTMAPPER.findAndRegisterModules();
    URL u = getCommandFile(this.getClass());
    try {
      CommandReader reader = OBJECTMAPPER.readValue(new File(u.getFile()), CommandReader.class);
      reader.setOptions(setOptions());
      return reader;
    } catch (IOException e) {
      throw new RuntimeException("Could not get fsadmin command docs", e);
    }
  }

  @Override
  public void writeDocumentation(File file) throws IOException {
    OBJECTMAPPER.writeValue(file, getDocs());
  }

  private String[] setOptions() {
    int n = 0;
    String[] opt = new String[this.getOptions().getOptions().size()];
    for (Option commandOpt:this.getOptions().getOptions()) {
      if (commandOpt.getOpt() == null) {
        opt[n] = "`--" + commandOpt.getLongOpt() + "` ";
      } else {
        opt[n] = "`-" + commandOpt.getOpt() + "` ";
      }
      opt[n] += commandOpt.getDescription();
      n++;
    }
    return opt;
  }
}

