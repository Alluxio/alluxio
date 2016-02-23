/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.hadoop.fs;

import alluxio.Constants;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * Reducer that accumulates values based on their type.
 * <p>
 * The type is specified in the key part of the key-value pair as a prefix to the key in the
 * following way
 * <p>
 * <tt>type:key</tt>
 * <p>
 * The values are accumulated according to the types:
 * <ul>
 * <li><tt>s:</tt> - string, concatenate</li>
 * <li><tt>f:</tt> - float, summ</li>
 * <li><tt>l:</tt> - long, summ</li>
 * </ul>
 *
 */
public class AccumulatingReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
  static final String VALUE_TYPE_LONG = "l:";
  static final String VALUE_TYPE_FLOAT = "f:";
  static final String VALUE_TYPE_STRING = "s:";
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  protected String mHostname;

  public AccumulatingReducer() {
    try {
      mHostname = java.net.InetAddress.getLocalHost().getHostName();
    } catch (Exception e) {
      mHostname = "localhost";
    }
    LOG.info("Starting AccumulatingReducer on " + mHostname);
  }

  public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output,
      Reporter reporter) throws IOException {
    String field = key.toString();

    reporter.setStatus("starting " + field + " ::host = " + mHostname);

    // concatenate strings
    if (field.startsWith(VALUE_TYPE_STRING)) {
      StringBuffer sSum = new StringBuffer();
      while (values.hasNext()) {
        sSum.append(values.next().toString()).append(";");
      }
      output.collect(key, new Text(sSum.toString()));
      reporter.setStatus("finished " + field + " ::host = " + mHostname);
      return;
    }
    // sum long values
    if (field.startsWith(VALUE_TYPE_FLOAT)) {
      float fSum = 0;
      while (values.hasNext()) {
        fSum += Float.parseFloat(values.next().toString());
      }
      output.collect(key, new Text(String.valueOf(fSum)));
      reporter.setStatus("finished " + field + " ::host = " + mHostname);
      return;
    }
    // sum long values
    if (field.startsWith(VALUE_TYPE_LONG)) {
      long lSum = 0;
      while (values.hasNext()) {
        lSum += Long.parseLong(values.next().toString());
      }
      output.collect(key, new Text(String.valueOf(lSum)));
    }
    reporter.setStatus("finished " + field + " ::host = " + mHostname);
  }
}
