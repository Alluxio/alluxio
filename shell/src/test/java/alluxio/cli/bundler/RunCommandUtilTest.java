package alluxio.cli.bundler;

import alluxio.AlluxioTestDirectory;

import alluxio.shell.CommandReturn;
import alluxio.util.ShellUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

public class RunCommandUtilTest {
  @Test
  public void runLinuxCommandNoFail() throws IOException {
    // create temp file
    File testDir = AlluxioTestDirectory.createTemporaryDirectory("command");
    File testFile = InfoCollectorTestUtils.createFileInDir(testDir, "testFile");

    // ls temp file
    String[] testCommand = new String[]{"ls", String.format("%s", testDir.getAbsolutePath())};
    CommandReturn cr = ShellUtils.execCommandWithOutput(testCommand);

    System.out.println(cr.getFormattedOutput());
    Assert.assertEquals(0, cr.getExitCode());
    Assert.assertTrue(cr.getOutput().contains(testFile.getName()));
  }

  @Test
  // TODO(jiacheng): How to ensure the Alluxio proc is there? integration test
  public void runAlluxioCommandNoFail() {
  }
}
