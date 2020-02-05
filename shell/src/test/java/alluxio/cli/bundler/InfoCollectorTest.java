package alluxio.cli.bundler;

import alluxio.AlluxioTestDirectory;
import alluxio.cli.bundler.command.AbstractInfoCollectorCommand;
import alluxio.conf.InstancedConfiguration;
import alluxio.cli.Command;
import alluxio.exception.AlluxioException;
import alluxio.util.ConfigurationUtils;

import org.junit.Test;
import org.reflections.Reflections;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class InfoCollectorTest {
  private static InstancedConfiguration mConf = new InstancedConfiguration(ConfigurationUtils.defaults());

  private int getNumberOfCommands() {
    Reflections reflections = new Reflections(AbstractInfoCollectorCommand.class.getPackage().getName());
    int cnt = 0;
    for (Class<? extends Command> cls : reflections.getSubTypesOf(Command.class)) {
      if (!Modifier.isAbstract(cls.getModifiers())) {
        cnt++;
      }
    }
    return cnt;
  }

  @Test
  public void loadedCommands() {
    InfoCollector ic = new InfoCollector(mConf);
    Collection<Command> commands = ic.getCommands();
    assertEquals(getNumberOfCommands(), commands.size());
  }

  @Test
  public void executeCommands() {
    InfoCollector ic = new InfoCollector(mConf);
    File targetDir = AlluxioTestDirectory.createTemporaryDirectory("testDir");
    ic.run("collectAll", targetDir.getAbsolutePath());

    // Find tarball
    File[] files = targetDir.listFiles();

    // TODO(jiacheng): Verify files for each command
    System.out.println(files.length + "files:");
    for (File f : files) {
      System.out.println(f.getName());
    }

    // The tarball is one extra file
    Collection<Command> commands = ic.getCommands();
    assertEquals(commands.size() + 1, files.length);
  }

  @Test
  public void runCommand() throws IOException, AlluxioException {
    InfoCollector shell = new InfoCollector(mConf);

    File targetDir = AlluxioTestDirectory.createTemporaryDirectory("testDir");
    String[] mockArgs = new String[]{"all", targetDir.getAbsolutePath()};

    shell.main(mockArgs);

    // The final result will be in a tarball
    File[] files = targetDir.listFiles();
    for (File f : files) {
      System.out.println(String.format("File %s", f.getName()));
      if (!f.isDirectory() && !f.getName().endsWith(".tar.gz")) {
        System.out.println(String.format("%s", new String(Files.readAllBytes(f.toPath()))));
      } else if (f.isDirectory()) {
        for (File ff : f.listFiles()) {
          System.out.println(String.format("Nested file %s size %s", ff.getName(), ff.getTotalSpace()));
//          System.out.println(String.format("%s", new String(Files.readAllBytes(ff.toPath()))));
        }
      }
    }
  }
}
