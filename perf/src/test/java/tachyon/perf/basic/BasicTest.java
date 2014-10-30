package tachyon.perf.basic;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.perf.benchmark.foo.FooTaskContext;
import tachyon.perf.benchmark.foo.FooTotalReport;

public class BasicTest {
  private final String J_TMP_DIR = System.getProperty("java.io.tmpdir");

  @After
  public final void after() {
    File xmlFile = new File(J_TMP_DIR + "/tachyon-perf-test/conf/test-case.xml");
    xmlFile.delete();
  }

  @Before
  public final void before() throws IOException {
    File tmpDir = new File(J_TMP_DIR + "/tachyon-perf-test/conf");
    tmpDir.mkdirs();
    File xmlFile = new File(J_TMP_DIR + "/tachyon-perf-test/conf/test-case.xml");
    xmlFile.delete();
    BufferedWriter fout = new BufferedWriter(new FileWriter(xmlFile));
    fout.write("<testCases>\n");
    fout.write("<type>\n");
    fout.write("<name>Foo</name>\n");
    fout.write("<taskClass>tachyon.perf.benchmark.foo.FooTask</taskClass>\n");
    fout.write("<taskContextClass>tachyon.perf.benchmark.foo.FooTaskContext</taskContextClass>\n");
    fout.write("<taskThreadClass>tachyon.perf.benchmark.foo.FooThread</taskThreadClass>\n");
    fout.write("<totalReportClass>tachyon.perf.benchmark.foo.FooTotalReport</totalReportClass>\n");
    fout.write("</type>\n");
    fout.write("</testCases>\n");
    fout.close();

    System.setProperty("tachyon.perf.home", J_TMP_DIR + "/tachyon-perf-test");
  }

  @Test
  public void taskWorkflowTest() throws Exception {
    String nodeName = "testNode";
    int taskId = 0;
    String testCase = "Foo";

    TaskConfiguration taskConf = TaskConfiguration.get(testCase, false);
    PerfTask task = TestCase.get().getTaskClass(testCase);
    task.initialSet(taskId, nodeName, taskConf, testCase);
    Assert.assertEquals(taskId, task.mId);
    Assert.assertEquals(nodeName, task.mNodeName);
    Assert.assertEquals(testCase, task.mTestCase);
    Assert.assertEquals("Foo", task.getCleanupDir());

    PerfTaskContext taskContext = TestCase.get().getTaskContextClass(testCase);
    taskContext.initialSet(taskId, nodeName, testCase, taskConf);
    Assert.assertEquals(taskId, taskContext.mId);
    Assert.assertEquals(nodeName, taskContext.mNodeName);
    Assert.assertEquals(testCase, taskContext.mTestCase);
    Assert.assertTrue(taskContext.mSuccess);

    Assert.assertTrue(task.setup(taskContext));
    Assert.assertTrue(task.run(taskContext));
    Assert.assertTrue(task.cleanup(taskContext));

    Assert.assertEquals(5, ((FooTaskContext) taskContext).getFoo());
    Assert.assertTrue(((FooTaskContext) taskContext).getThreads());
    Assert.assertTrue(((FooTaskContext) taskContext).getWrite());

    PerfTotalReport totalReport = TestCase.get().getTotalReportClass(testCase);
    totalReport.initialSet(testCase);
    Assert.assertEquals(testCase, totalReport.mTestCase);

    PerfTaskContext[] taskContexts = new PerfTaskContext[3];
    for (int i = 0; i < taskContexts.length; i ++) {
      taskContexts[i] = taskContext;
    }
    totalReport.initialFromTaskContexts(taskContexts);
    totalReport.writeToFile(null);
    Assert.assertEquals(15, ((FooTotalReport) totalReport).getFoo());
    Assert.assertTrue(((FooTotalReport) totalReport).getWrite());
  }
}
