package alluxio.cli.bundler;

import alluxio.cli.bundler.command.AbstractInfoCollectorCommand;
import alluxio.conf.InstancedConfiguration;
import alluxio.cli.Command;
import alluxio.util.ConfigurationUtils;

import org.junit.Assert;
import org.junit.Test;
import org.reflections.Reflections;

import java.lang.reflect.Modifier;
import java.util.Collection;

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
    Assert.assertEquals(getNumberOfCommands(), commands.size());
  }
}
