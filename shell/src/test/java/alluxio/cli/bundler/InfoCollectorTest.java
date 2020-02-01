package alluxio.cli.bundler;

import alluxio.AlluxioTestDirectory;
import alluxio.cli.bundler.command.AbstractInfoCollectorCommand;
import alluxio.conf.InstancedConfiguration;
import alluxio.cli.Command;
import alluxio.util.ConfigurationUtils;

import org.junit.Assert;
import org.junit.Test;
import org.reflections.Reflections;

import java.io.File;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

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

}
