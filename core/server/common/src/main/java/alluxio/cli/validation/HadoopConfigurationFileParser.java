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

package alluxio.cli.validation;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;

import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;

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
  public Map<String, String> parseXmlConfiguration(final String path) {
    File xmlFile;
    xmlFile = new File(path);
    if (!xmlFile.exists()) {
      System.err.format("File %s does not exist.", path);
      return null;
    }
    DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder docBuilder;
    try {
      docBuilder = docBuilderFactory.newDocumentBuilder();
    } catch (ParserConfigurationException e) {
      System.err.format("Failed to create instance of DocumentBuilder for file: %s. %s. %n",
          path, e.getMessage());
      return null;
    }
    Document doc;
    try {
      doc = docBuilder.parse(xmlFile);
    } catch (IOException e) {
      System.err.format("An I/O error occured reading file %s. %s.%n", path, e.getMessage());
      return null;
    } catch (SAXException e) {
      System.err.format("A parsing error occured parsing file %s. %s.%n", path, e.getMessage());
      return null;
    }
    // Optional, but recommended.
    // Refer to http://stackoverflow.com/questions/13786607/normalization-in-dom-parsing-with-java-how-does-it-work
    doc.getDocumentElement().normalize();
    Map<String, String> ret = new HashMap<>();
    NodeList propNodeList = doc.getElementsByTagName("property");
    for (int i = 0; i < propNodeList.getLength(); i++) {
      Node propNode = propNodeList.item(i);
      if (propNode.getNodeType() == Node.ELEMENT_NODE) {
        Element element = (Element) propNode;
        ret.put(element.getElementsByTagName("name").item(0).getTextContent(),
            element.getElementsByTagName("value").item(0).getTextContent());
      }
    }
    return ret;
  }
}
