package tachyon.perf.benchmark;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.perf.basic.PerfThread;
import tachyon.perf.basic.TaskConfiguration;

public class SimpleTaskContextTest {
  private final String J_TMP_DIR = System.getProperty("java.io.tmpdir");

  @After
  public final void after() {
    File contextFile = new File(J_TMP_DIR + "/tachyon-perf-test/context-test");
    contextFile.delete();
  }

  @Before
  public final void before() throws IOException {
    File tmpDir = new File(J_TMP_DIR + "/tachyon-perf-test");
    tmpDir.mkdirs();
  }

  @Test
  public void writeLoadTest() throws IOException {
    TaskConfiguration taskConf = TaskConfiguration.get("Foo", false);
    SimpleTaskContext context1 = new SimpleTaskContext();
    context1.initialSet(0, "", "Foo", taskConf);
    context1.setFromThread(new PerfThread[1]);
    File contextFile = new File(J_TMP_DIR + "/tachyon-perf-test/context-test");
    context1.writeToFile(contextFile);
    SimpleTaskContext context2 = new SimpleTaskContext();
    context2.loadFromFile(contextFile);

    Assert.assertEquals(context1.getTestCase(), context2.getTestCase());
    Assert.assertEquals(context1.getId(), context2.getId());
    Assert.assertEquals(context1.getNodeName(), context2.getNodeName());
    Assert.assertEquals(context1.getSuccess(), context2.getSuccess());
    Assert.assertEquals(context1.getStartTimeMs(), context2.getStartTimeMs());
    Assert.assertEquals(context1.getFinishTimeMs(), context2.getFinishTimeMs());

    Assert.assertEquals(context1.mConf, context2.mConf);
    Assert.assertEquals(context1.mAdditiveStatistics, context2.mAdditiveStatistics);
  }
}
