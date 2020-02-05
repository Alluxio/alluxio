package alluxio.cli.bundler.command;

import alluxio.cli.Command;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.util.ConfigurationUtils;
import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;

public abstract class AbstractInfoCollectorCommand implements Command {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractInfoCollectorCommand.class);
  private static final String FILE_NAME_SUFFIX = ".txt";

  FileSystemContext mFsContext;

  public AbstractInfoCollectorCommand(@Nullable FileSystemContext fsContext) {
    if (fsContext == null) {
      fsContext =
              FileSystemContext.create(new InstancedConfiguration(ConfigurationUtils.defaults()));
    }
    mFsContext = fsContext;
  }

  public static String getDestDir(CommandLine cl) {
    String[] args = cl.getArgs();
    return args[0];
  }

  public String getWorkingDirectory(String baseDirPath) {
    String workingDirPath =  Paths.get(baseDirPath, this.getCommandName()).toString();
    createWorkingDirIfNotExisting(workingDirPath);
    return workingDirPath;
  }

  public boolean foundPreviousWork(String baseDirPath) {
    String workingDirPath = getWorkingDirectory(baseDirPath);
    // TODO(jiacheng): this is wrong!
    File workingDir = new File(workingDirPath);

    // TODO(jiacheng): better idea?
    // If the working directory is not empty, assume previous work can be reused.
    if (workingDir.list().length == 0) {
      return false;
    }
    LOG.info(String.format("Working dir %s is not empty. Assume previous work has completed.",
            workingDirPath));
    return true;
  }

  public File generateOutputFile(String baseDirPath, String fileName) throws IOException {
    if (!fileName.endsWith(FILE_NAME_SUFFIX)) {
      fileName += FILE_NAME_SUFFIX;
    }
    String outputFilePath = Paths.get(getWorkingDirectory(baseDirPath), fileName).toString();
    File outputFile = new File(outputFilePath);
    if (!outputFile.exists()) {
      outputFile.createNewFile();
    }
    return outputFile;
  }

  public File getOutputFile(CommandLine cl) {
    return new File(getDestDir(cl), this.getCommandName());
  }

  private void createWorkingDirIfNotExisting(String path) {
    // mkdirs checks existence of the path
    File workingDir = new File(path);
    workingDir.mkdirs();
  }
}
