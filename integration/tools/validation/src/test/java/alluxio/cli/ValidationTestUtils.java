/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.cli;

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

/**
 * A utility class to hold utility methods for tests for validation tasks.
 * */
public class ValidationTestUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ValidationTestUtils.class);

  /**
   * Creates an XML configuration file for Hadoop on the specified path.
   *
   * @param path the configuration file path
   * @param properties the properties to hold in the configuration
   * */
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
      // TODO(bradyoo): Swallowing all exceptions and merely printing is not okay!
      e.printStackTrace();
    }
  }

  private static Element createElement(Document doc, String k, String v) {
    Element name = doc.createElement("name");
    name.appendChild(doc.createTextNode(k));
    Element value = doc.createElement("value");
    value.appendChild(doc.createTextNode(v));
    Element prop = doc.createElement("property");
    prop.appendChild(name);
    prop.appendChild(value);
    return prop;
  }

  /**
   * Prepares core-site.xml and hdfs-site.xml files for a test.
   * */
  public static File prepareConfDir() throws IOException {
    // The dir path will contain randomness so will be different every time
    File testConfDir = createTemporaryDirectory();
    createFileInDir(testConfDir, "core-site.xml");
    createFileInDir(testConfDir, "hdfs-site.xml");
    return testConfDir;
  }

  /**
   * Creates a temporary directory for the test.
   * */
  public static File createTemporaryDirectory() {
    final File file = Files.createTempDir();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        FileUtils.deleteDirectory(file);
      } catch (IOException e) {
        LOG.warn("Failed to clean up {} : {}", file.getAbsolutePath(), e.toString());
      }
    }));
    return file;
  }

  public static File createFileInDir(File dir, String fileName) throws IOException {
    File newFile = new File(Paths.get(dir.getAbsolutePath(), fileName).toString());
    newFile.createNewFile();
    return newFile;
  }
}
