package tachyon.perf.basic;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TaskConfigurationTest {
  private final String J_TMP_DIR = System.getProperty("java.io.tmpdir");

  @After
  public final void after() {
    File xmlFile = new File(J_TMP_DIR + "/tachyon-perf-test/conf/testsuite/Foo.xml");
    xmlFile.delete();
  }

  @Before
  public final void before() throws IOException {
    File tmpDir = new File(J_TMP_DIR + "/tachyon-perf-test/conf/testsuite");
    tmpDir.mkdirs();
    File xmlFile = new File(J_TMP_DIR + "/tachyon-perf-test/conf/testsuite/Foo.xml");
    xmlFile.delete();
    BufferedWriter fout = new BufferedWriter(new FileWriter(xmlFile));
    fout.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    fout.write("<configuration>\n");
    fout.write("<property>\n");
    fout.write("<name>xxx</name>\n");
    fout.write("<value>yyy</value>\n");
    fout.write("</property>\n");
    fout.write("</configuration>\n");
    fout.close();

    System.setProperty("tachyon.perf.home", J_TMP_DIR + "/tachyon-perf-test");
  }

  @Test
  public void parseFileTest() throws Exception {
    TaskConfiguration taskConf =
        new TaskConfiguration(J_TMP_DIR + "/tachyon-perf-test/conf/testsuite/Foo.xml");
    Assert.assertEquals(1, taskConf.getAllProperties().size());
    Assert.assertEquals("yyy", taskConf.getProperty("xxx"));
  }

  @Test
  public void getSetTest() {
    TaskConfiguration taskConf = new TaskConfiguration();
    taskConf.addProperty("pro.int", "123");
    taskConf.addProperty("pro.boolean", "true");
    taskConf.addProperty("pro.long", "9876543210");
    taskConf.addProperty("pro.string", "foo");
    Assert.assertEquals(123, taskConf.getIntProperty("pro.int"));
    Assert.assertTrue(taskConf.getBooleanProperty("pro.boolean"));
    Assert.assertEquals(9876543210L, taskConf.getLongProperty("pro.long"));
    Assert.assertEquals("foo", taskConf.getProperty("pro.string"));
  }
}
