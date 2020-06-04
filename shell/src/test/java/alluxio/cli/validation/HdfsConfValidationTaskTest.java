package alluxio.cli.validation;

import alluxio.cli.ValidateUtils;
import alluxio.cli.bundler.InfoCollectorTestUtils;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;

import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class HdfsConfValidationTaskTest {
  private static InstancedConfiguration sConf;
  private static File sTestDir;

  @BeforeClass
  public static void initConf() throws IOException {
    sTestDir = prepareConfDir();
    sConf = InstancedConfiguration.defaults();
    sConf.set(PropertyKey.CONF_DIR, sTestDir.getAbsolutePath());
  }

  // Prepare a temp dir with some log files
  public static File prepareConfDir() throws IOException {
    // The dir path will contain randomness so will be different every time
    // TODO(jiacheng): move this util
    File testConfDir = InfoCollectorTestUtils.createTemporaryDirectory();
    InfoCollectorTestUtils.createFileInDir(testConfDir, "core-site.xml");
    InfoCollectorTestUtils.createFileInDir(testConfDir, "hdfs-site.xml");
    return testConfDir;
  }

  public static Element createElement(Document doc, String k, String v) {
    Element name = doc.createElement("name");
    name.appendChild(doc.createTextNode(k));
    Element value = doc.createElement("value");
    value.appendChild(doc.createTextNode(v));
    Element prop = doc.createElement("property");
    prop.appendChild(name);
    prop.appendChild(value);
    return prop;
  }

  public static void writeXML(String path, Map<String, String> properties) {
    // Example:
    // <configuration>
    //   <property>
    //     <name>dfs.nameservices</name>
    //     <value>hanameservice</value>
    //   </property>
    // </configuration>
    try {
      File f = new File(path);
      if (!f.exists()) {
        System.out.format("Creating file %s%n", f.getAbsolutePath());
        f.createNewFile();
      }

      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = factory.newDocumentBuilder();
      Document doc = builder.newDocument();

      Element conf = doc.createElement("configuration");
      doc.appendChild(conf);

      for (Map.Entry<String, String> entry : properties.entrySet()) {
        Element prop = createElement(doc, entry.getKey(), entry.getValue());
        conf.appendChild(prop);
      }

      TransformerFactory transformerfactory = TransformerFactory.newInstance();
      Transformer transformer = transformerfactory.newTransformer();
      DOMSource source = new DOMSource(doc);
      StreamResult sResult = new StreamResult(f);
      transformer.transform(source, sResult);
     } catch (Exception e) {
       e.printStackTrace();
    }
  }

  @Test
  public void loadedConf() {
    String hdfsSite = Paths.get(sTestDir.toPath().toString(), "hdfs-site.xml").toString();
    writeXML(hdfsSite, ImmutableMap.of("key2", "value2"));

    String coreSite = Paths.get(sTestDir.toPath().toString(), "core-site.xml").toString();
    writeXML(coreSite, ImmutableMap.of("key1", "value1"));

    sConf.set(PropertyKey.UNDERFS_HDFS_CONFIGURATION, hdfsSite + HdfsConfValidationTask.SEPARATOR + coreSite);
    HdfsConfValidationTask task = new HdfsConfValidationTask("hdfs://namenode:9000/alluxio", sConf);
    ValidateUtils.TaskResult result = task.loadHdfsConfig();
    assertEquals(result.getState(), ValidateUtils.State.OK);
  }

  @Test
  public void missingCoreSiteXML() {
    // Only prepare hdfs-site.xml
    String hdfsSite = Paths.get(sTestDir.toPath().toString(), "hdfs-site.xml").toString();
    writeXML(hdfsSite, ImmutableMap.of("key1", "value1"));

    sConf.set(PropertyKey.UNDERFS_HDFS_CONFIGURATION, hdfsSite);
    HdfsConfValidationTask task = new HdfsConfValidationTask("hdfs://namenode:9000/alluxio", sConf);
    ValidateUtils.TaskResult result = task.loadHdfsConfig();
    assertEquals(result.getState(), ValidateUtils.State.FAILED);
    assertThat(result.getResult(), containsString("core-site.xml is not configured"));
  }

  @Test
  public void missingHdfsSiteXML() {
    // Only prepare core-site.xml
    String coreSite = Paths.get(sTestDir.toPath().toString(), "core-site.xml").toString();
    writeXML(coreSite, ImmutableMap.of("key1", "value1"));

    sConf.set(PropertyKey.UNDERFS_HDFS_CONFIGURATION, coreSite);
    HdfsConfValidationTask task = new HdfsConfValidationTask("hdfs://namenode:9000/alluxio", sConf);
    ValidateUtils.TaskResult result = task.loadHdfsConfig();
    assertEquals(result.getState(), ValidateUtils.State.FAILED);
    assertThat(result.getResult(), containsString("hdfs-site.xml is not configured"));
  }

  @Test
  public void inconsistentConf() {
    String hdfsSite = Paths.get(sTestDir.toPath().toString(), "hdfs-site.xml").toString();
    writeXML(hdfsSite, ImmutableMap.of("key1", "value2"));

    String coreSite = Paths.get(sTestDir.toPath().toString(), "core-site.xml").toString();
    writeXML(coreSite, ImmutableMap.of("key1", "value1"));

    sConf.set(PropertyKey.UNDERFS_HDFS_CONFIGURATION, hdfsSite + HdfsConfValidationTask.SEPARATOR + coreSite);
    HdfsConfValidationTask task = new HdfsConfValidationTask("hdfs://namenode:9000/alluxio", sConf);
    ValidateUtils.TaskResult result = task.validate(ImmutableMap.of());
    System.out.println(result);

    assertEquals(ValidateUtils.State.FAILED, result.getState());
    assertThat(result.getResult(), containsString("key1"));
    assertThat(result.getResult(), containsString("value1 in core-site.xml"));
    assertThat(result.getResult(), containsString("value2 in hdfs-site.xml"));
    assertThat(result.getAdvice(), containsString("fix the inconsistency"));
  }

  @Test
  public void valieConf() {
    String hdfsSite = Paths.get(sTestDir.toPath().toString(), "hdfs-site.xml").toString();
    writeXML(hdfsSite, ImmutableMap.of("key1", "value1", "key3", "value3"));

    String coreSite = Paths.get(sTestDir.toPath().toString(), "core-site.xml").toString();
    writeXML(coreSite, ImmutableMap.of("key1", "value1", "key4", "value4"));

    sConf.set(PropertyKey.UNDERFS_HDFS_CONFIGURATION, hdfsSite + HdfsConfValidationTask.SEPARATOR + coreSite);
    HdfsConfValidationTask task = new HdfsConfValidationTask("hdfs://namenode:9000/alluxio", sConf);
    ValidateUtils.TaskResult result = task.validate(ImmutableMap.of());
    System.out.println(result);

    assertEquals(ValidateUtils.State.OK, result.getState());
  }

  @After
  public void resetConf() {
    sConf.unset(PropertyKey.UNDERFS_HDFS_CONFIGURATION);
  }

  @After
  public void removeConfFiles(){
    File hdfsSite = new File(sTestDir, "hdfs-site.xml");
    if (hdfsSite.exists()) {
      hdfsSite.delete();
    }
    File coreSite = new File(sTestDir, "core-site.xml");
    if (coreSite.exists()) {
      coreSite.delete();
    }
  }
}
