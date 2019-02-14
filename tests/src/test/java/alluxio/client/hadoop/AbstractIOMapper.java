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

package alluxio.client.hadoop;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;

import javax.annotation.Nullable;

/**
 * Base mapper class for IO operations.
 * <p>
 * Two abstract methods {@link #doIO(Reporter, String, long)} and
 * {@link #collectStats(OutputCollector, String,long, Object)} should be
 * overloaded in derived classes to define the IO operation and the statistics data to be collected
 * by subsequent reducers.
 *
 */
public abstract class AbstractIOMapper<T> extends Configured implements
    Mapper<Text, LongWritable, Text, Text> {

  protected byte[] mBuffer;
  protected int mBufferSize;
  protected FileSystem mFS;
  protected String mHostname;
  protected Closeable mStream;

  public AbstractIOMapper() {}

  @Override
  public void configure(JobConf conf) {
    setConf(conf);
    try {
      mFS = FileSystem.get(conf);
    } catch (Exception e) {
      throw new RuntimeException("Cannot create file system.", e);
    }
    mBufferSize = conf.getInt("test.io.file.buffer.size", 4096);
    mBuffer = new byte[mBufferSize];
    try {
      mHostname = InetAddress.getLocalHost().getHostName();
    } catch (Exception e) {
      mHostname = "localhost";
    }
  }

  @Override
  public void close() throws IOException {}

  /**
   * Perform io operation, usually read or write.
   *
   * @param reporter
   * @param name file name
   * @param value offset within the file
   * @return object that is passed as a parameter to
   *         {@link #collectStats(OutputCollector, String,long, Object)}
   */
  abstract T doIO(Reporter reporter, String name, long value) throws IOException;

  /**
   * Create an input or output stream based on the specified file. Subclasses should override this
   * method to provide an actual stream.
   *
   * @param name file name
   * @return the stream
   */
  @Nullable
  public Closeable getIOStream(String name) throws IOException {
    return null;
  }

  /**
   * Collect stat data to be combined by a subsequent reducer.
   *
   * @param output
   * @param name file name
   * @param execTime IO execution time
   * @param doIOReturnValue value returned by
   *        {@link #doIO(Reporter, String,long)}
   */
  abstract void collectStats(OutputCollector<Text, Text> output, String name, long execTime,
      T doIOReturnValue) throws IOException;

  /**
   * Map file name and offset into statistical data.
   * <p>
   * The map task is to get the <tt>key</tt>, which contains the file name, and the <tt>value</tt>,
   * which is the offset within the file.
   *
   * The parameters are passed to the abstract method
   * {@link #doIO(Reporter, String,long)}, which performs the io operation,
   * usually read or write data, and then
   * {@link #collectStats(OutputCollector, String,long, Object)} is called
   * to prepare stat data for a subsequent reducer.
   */
  @Override
  public void map(Text key, LongWritable value, OutputCollector<Text, Text> output,
      Reporter reporter) throws IOException {
    String name = key.toString();
    long longValue = value.get();

    reporter.setStatus("starting " + name + " ::host = " + mHostname);

    mStream = getIOStream(name);
    T statValue = null;
    long tStart = System.currentTimeMillis();
    try {
      statValue = doIO(reporter, name, longValue);
    } finally {
      if (mStream != null) {
        mStream.close();
      }
    }
    long tEnd = System.currentTimeMillis();
    long execTime = tEnd - tStart;
    collectStats(output, name, execTime, statValue);

    reporter.setStatus("finished " + name + " ::host = " + mHostname);
  }
}
