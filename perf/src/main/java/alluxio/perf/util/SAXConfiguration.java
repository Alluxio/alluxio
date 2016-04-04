/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.perf.util;

import java.util.HashMap;
import java.util.Map;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * This class is used to parse the configuration file for specified benchmark.
 */
public class SAXConfiguration extends DefaultHandler {
  private String mCurrentName = null;
  private String mCurrentTag = null;
  private Map<String, String> mProperties;

  public Map<String, String> getProperties() {
    return mProperties;
  }

  @Override
  public void characters(char[] ch, int start, int length) throws SAXException {
    if (mCurrentTag != null) {
      String content = new String(ch, start, length);
      if ("name".equals(mCurrentTag)) {
        mCurrentName = content;
      } else if ("value".equals(mCurrentTag)) {
        mProperties.put(mCurrentName, content);
      }
    }
  }

  @Override
  public void endElement(String uri, String localName, String qName) throws SAXException {
    if ("property".equals(qName)) {
      mCurrentName = null;
    }
    mCurrentTag = null;
  }

  @Override
  public void startDocument() throws SAXException {
    mProperties = new HashMap<String, String>();
  }

  @Override
  public void startElement(String uri, String localName, String qName, Attributes attributes)
      throws SAXException {
    mCurrentTag = qName;
  }
}
