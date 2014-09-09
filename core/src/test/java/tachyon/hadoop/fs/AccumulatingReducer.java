package tachyon.hadoop.fs;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * Reducer that accumulates values based on their type.
 * <p>
 * The type is specified in the key part of the key-value pair 
 * as a prefix to the key in the following way
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
@SuppressWarnings("deprecation")
public class AccumulatingReducer extends MapReduceBase
    implements Reducer<Text, Text, Text, Text> {
  static final String VALUE_TYPE_LONG = "l:";
  static final String VALUE_TYPE_FLOAT = "f:";
  static final String VALUE_TYPE_STRING = "s:";
  private static final Log LOG = LogFactory.getLog(AccumulatingReducer.class);
  
  protected String hostName;
  
  public AccumulatingReducer () {
    try {
      hostName = java.net.InetAddress.getLocalHost().getHostName();
    } catch(Exception e) {
      hostName = "localhost";
    }
    LOG.info("Starting AccumulatingReducer on " + hostName);
  }
  
  public void reduce(Text key,
                     Iterator<Text> values,
                     OutputCollector<Text, Text> output,
                     Reporter reporter
                     ) throws IOException {
    String field = key.toString();

    reporter.setStatus("starting " + field + " ::host = " + hostName);

    // concatenate strings
    if (field.startsWith(VALUE_TYPE_STRING)) {
      StringBuffer sSum = new StringBuffer();
      while (values.hasNext())
        sSum.append(values.next().toString()).append(";");
      output.collect(key, new Text(sSum.toString()));
      reporter.setStatus("finished " + field + " ::host = " + hostName);
      return;
    }
    // sum long values
    if (field.startsWith(VALUE_TYPE_FLOAT)) {
      float fSum = 0;
      while (values.hasNext())
        fSum += Float.parseFloat(values.next().toString());
      output.collect(key, new Text(String.valueOf(fSum)));
      reporter.setStatus("finished " + field + " ::host = " + hostName);
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
    reporter.setStatus("finished " + field + " ::host = " + hostName);
  }
}
