package alluxio.cli.fs.command;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.AlluxioException;
import javax.annotation.Nullable;
import org.apache.commons.cli.CommandLine;

import java.io.IOException;

public class CRC64CheckCommand extends AbstractFileSystemCommand{
  public CRC64CheckCommand(
      @Nullable FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  protected void runPlainPath(AlluxioURI plainPath, CommandLine cl)
      throws AlluxioException, IOException {
    /*
    System.out.println("Checking " + plainPath);
    long crc64 = CRC64CheckCommandUtils.checkCRC64(mFsContext, mFileSystem, plainPath);
    System.out.println("CRC64 check for file " + plainPath + " succeeded. "
        + "CRC64: " + crc64);

     */
    System.out.println(CRC64CheckCommandUtils.calculateAlluxioCRC64(mFsContext, mFileSystem, plainPath));
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    for (String dirArg : args) {
      AlluxioURI path = new AlluxioURI(dirArg);
      runPlainPath(path, cl);
    }
    return 0;
  }

  @Override
  public String getCommandName() {
    return "crc64check";
  }

  @Override
  public String getUsage() {
    return "crc64check <path> ...";
  }

  @Override
  public String getDescription() {
    return "Does the CRC check on a given alluxio path. The UFS must support CRC64 checksum and "
        + "the file must be fully cached on alluxio.";
  }
}
