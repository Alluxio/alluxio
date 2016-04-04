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
 * This class is used to parse the conf/.xml.
 */
public class SAXTestCase extends DefaultHandler {
  private String mCurrentTag = null;
  private String mCurrentType = null;
  private Map<String, String> mTaskClasses;
  private Map<String, String> mTaskContextClasses;
  private Map<String, String> mTaskThreadClasses;
  private Map<String, String> mTotalReportClasses;

  public Map<String, String> getTaskClasses() {
    return mTaskClasses;
  }

  public Map<String, String> getTaskContextClasses() {
    return mTaskContextClasses;
  }

  public Map<String, String> getTaskThreadClasses() {
    return mTaskThreadClasses;
  }

  public Map<String, String> getTotalReportClasses() {
    return mTotalReportClasses;
  }

  @Override
  public void characters(char[] ch, int start, int length) throws SAXException {
    if (mCurrentTag != null) {
      String content = new String(ch, start, length);
      if ("name".equals(mCurrentTag)) {
        mCurrentType = content;
      } else if ("taskClass".equals(mCurrentTag)) {
        mTaskClasses.put(mCurrentType, content);
      } else if ("taskContextClass".equals(mCurrentTag)) {
        mTaskContextClasses.put(mCurrentType, content);
      } else if ("taskThreadClass".equals(mCurrentTag)) {
        mTaskThreadClasses.put(mCurrentType, content);
      } else if ("totalReportClass".equals(mCurrentTag)) {
        mTotalReportClasses.put(mCurrentType, content);
      }
    }
  }

  @Override
  public void endElement(String uri, String localName, String qName) throws SAXException {
    if ("type".equals(qName)) {
      mCurrentType = null;
    }
    mCurrentTag = null;
  }

  @Override
  public void startDocument() throws SAXException {
    mTaskClasses = new HashMap<String, String>();
    mTaskContextClasses = new HashMap<String, String>();
    mTaskThreadClasses = new HashMap<String, String>();
    mTotalReportClasses = new HashMap<String, String>();
  }

  @Override
  public void startElement(String uri, String localName, String qName, Attributes attributes)
      throws SAXException {
    mCurrentTag = qName;
  }
}
