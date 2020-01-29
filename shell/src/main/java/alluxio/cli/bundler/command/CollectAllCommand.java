package alluxio.cli.bundler.command;

import alluxio.client.file.FileSystemContext;
import alluxio.exception.AlluxioException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

// TODO(jiacheng): Do we want to move this logic to InfoCollector shell and have finer granularity?
public class CollectAllCommand extends AbstractInfoCollectorCommand {
  private static final Logger LOG = LoggerFactory.getLogger(CollectAllCommand.class);

  private static final Option FORCE_OPTION =
          Option.builder("f")
                  .required(false)
                  .hasArg(false)
                  .desc("ignores existing work")
                  .build();

  private static final Option COMPONENT_OPTION =
          Option.builder("c").longOpt("components")
                  .hasArg(true).desc("components to collect logs from")
                  .build();

  public CollectAllCommand(@Nullable FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public Options getOptions() {
    return new Options()
            .addOption(FORCE_OPTION)
            .addOption(COMPONENT_OPTION);
  }

  // TODO(jiacheng): What does this do?
  private static final String CRUNCHIFY_BASEDIR = "";
  private static final String CRUNCHIFY_PATH = "/bundle.tar.zip";

  //
  List<AbstractInfoCollectorCommand> mChildren;

  @Override
  public String getCommandName() {
    return "collectAll";
  }

  // https://crunchify.com/how-to-create-zip-or-tar-programatically-in-java-using-apache-commons-archivers-and-compressors/
  // TODO(jiacheng): This method needs rewriting
  public static void crunchfyArchive(String srcPath) throws FileNotFoundException, IOException {
    File crunchifySourceFile = new File(srcPath);

    // Returns the name of the file or directory denoted by this abstract pathname
    String crunchifyFileName = crunchifySourceFile.getName();

    // Returns the pathname string of this abstract pathname's parent
    String crunchifyBaseFileNamePath = crunchifySourceFile.getParent();

    String destPath = crunchifyBaseFileNamePath + File.separator + crunchifyFileName + ".zip";

    TarArchiveOutputStream outputStream = new TarArchiveOutputStream(
            new FileOutputStream(new File(destPath)));

    crunchfyArchive(crunchifySourceFile, outputStream, CRUNCHIFY_BASEDIR);

    // Flushes this output stream and forces any buffered output bytes to be written out
    outputStream.flush();

    // Closes the underlying OutputStream
    outputStream.close();
  }

  private static void crunchfyArchive(File crunchifySourceFile,
                                      TarArchiveOutputStream outputStream, String crunchifyBasePath) throws IOException {
    if (crunchifySourceFile.isDirectory()) {

      // Archive Directory
      archiveCrunchifyDirectory(crunchifySourceFile, outputStream, crunchifyBasePath);
    } else {

      // Archive File
      archiveCrunchifyFile(crunchifySourceFile, outputStream, crunchifyBasePath);
    }
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    int ret = 0;

    // Determine the working dir path
    String targetDir = getDestDir(cl);

    // Invoke all other commands to collect information
    // FORCE_OPTION will be propagated to child commands
    for (AbstractInfoCollectorCommand child : mChildren) {
      LOG.info(String.format("Executing command %s", child.getCommandName()));
      ret = child.run(cl);
      LOG.info(String.format("Command return %s", ret));
    }

    // Generate bundle
    LOG.info(String.format("Archiving dir %s", targetDir));
    crunchfyArchive(targetDir);
    LOG.info("Archiving finished");

    return ret;
  }

  private static void archiveCrunchifyDirectory(File crunchifyDirectory,
                                                TarArchiveOutputStream outputStream, String crunchifyBasePath) throws IOException {

    // Returns an array of abstract pathnames denoting the files in the directory denoted by this abstract pathname
    File[] crunchifyFiles = crunchifyDirectory.listFiles();

    if (crunchifyFiles != null) {
      if (crunchifyFiles.length < 1) {

        // Construct an entry with only a name. This allows the programmer to construct the entry's header "by hand". File
        // is set to null
        TarArchiveEntry entry = new TarArchiveEntry(
                crunchifyBasePath + crunchifyDirectory.getName() + CRUNCHIFY_PATH);

        // Put an entry on the output stream
        outputStream.putArchiveEntry(entry);

        // Close an entry. This method MUST be called for all file entries that contain data
        outputStream.closeArchiveEntry();
      }

      // Repeat for all files
      for (File crunchifyFile : crunchifyFiles) {

        crunchfyArchive(crunchifyFile, outputStream,
                crunchifyBasePath + crunchifyDirectory.getName() + CRUNCHIFY_PATH);

      }
    }
  }

  private static void archiveCrunchifyFile(File crunchifyFile,
                                           TarArchiveOutputStream outputStream, String crunchifyDirectory) throws IOException {

    TarArchiveEntry crunchifyEntry = new TarArchiveEntry(
            crunchifyDirectory + crunchifyFile.getName());

    // Set this entry's file size
    crunchifyEntry.setSize(crunchifyFile.length());

    outputStream.putArchiveEntry(crunchifyEntry);

    BufferedInputStream inputStream = new BufferedInputStream(
            new FileInputStream(crunchifyFile));
    int counter;

    // 512: buffer size
    byte byteData[] = new byte[512];
    while ((counter = inputStream.read(byteData, 0, 512)) != -1) {
      outputStream.write(byteData, 0, counter);
    }

    inputStream.close();
    outputStream.closeArchiveEntry();
  }


  @Override
  public String getUsage() {
    return "collectAll";
  }

  @Override
  // TODO(jiacheng): desc
  public String getDescription() {
    return "Collect all information.";
  }
}
