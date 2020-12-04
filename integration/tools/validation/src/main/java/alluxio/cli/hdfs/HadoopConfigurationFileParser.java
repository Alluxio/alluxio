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

package alluxio.cli.hdfs;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Parser for configuration files.
 */
public class HadoopConfigurationFileParser {
  /**
   * Constructs a {@link HadoopConfigurationFileParser} object.
   */
  public HadoopConfigurationFileParser() {}

  /**
   * Parse an xml configuration file into a map.
   *
   * Referred to https://www.mkyong.com/java/how-to-read-xml-file-in-java-dom-parser/
   *
   * @param path path to the xml file
   * @return Map from property names to values
   */
  public Map<String, String> parseXmlConfiguration(final String path) throws IOException {
    File xmlFile;
    xmlFile = new File(path);
    if (!xmlFile.exists()) {
      throw new IOException(String.format("File %s does not exist.", path));
    }
    org.apache.hadoop.conf.Configuration c = new org.apache.hadoop.conf.Configuration(false);
    c.addResource(Files.newInputStream(Paths.get(path)));
    Map<String, String> properties = new HashMap<>();
    c.iterator().forEachRemaining(e -> properties.put(e.getKey(), e.getValue()));
    return properties;
  }
}
