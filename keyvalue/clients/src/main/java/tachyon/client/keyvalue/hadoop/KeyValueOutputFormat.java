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

package tachyon.client.keyvalue.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import tachyon.TachyonURI;
import tachyon.annotation.PublicApi;
import tachyon.client.keyvalue.KeyValueStores;
import tachyon.client.keyvalue.KeyValueStoreWriter;
import tachyon.exception.TachyonException;

/**
 * An {@link OutputFormat} implementation to let MapReduce job write to a key-value store under the
 * MapReduce output directory.
 * <p>
 * There are different key-value stores under the output directory configured via MapReduce property
 * {@code mapreduce.output.fileoutputformat.outputdir} for different tasks. The stores are merged
 * into one key-value store under the MapReduce output directory by {@link KeyValueOutputCommitter}.
 *
 * TODO(cc): Consider key distributions in each Reducer.
 */
@PublicApi
public class KeyValueOutputFormat extends FileOutputFormat<BytesWritable, BytesWritable> {
  @Override
  public RecordWriter<BytesWritable, BytesWritable> getRecordWriter(FileSystem ignored,
      JobConf conf, String name, Progressable progress) throws IOException {
    Path outputPath = FileOutputFormat.getTaskOutputPath(conf, name);
    KeyValueStores kvStore = KeyValueStores.Factory.create();
    try {
      KeyValueStoreWriter writer = kvStore.create(new TachyonURI(outputPath.toString()));
      return new KeyValueRecordWriter(writer);
    } catch (TachyonException e) {
      throw new IOException(e);
    }
  }

  /**
   * {@inheritDoc}
   * <p>
   * {@link KeyValueOutputCommitter} is forced to be used to mergeAndDelete key-value stores created
   * by each Reducer into one key-value store under the MapReduce output directory.
   * <p>
   * NOTE: This method is called immediately when job is submitted, so that modifications to the
   * {@link JobConf} are reflected in the whole job.
   */
  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf conf)
      throws FileAlreadyExistsException, InvalidJobConfException, IOException {
    super.checkOutputSpecs(ignored, conf);
    conf.setOutputCommitter(KeyValueOutputCommitter.class);
  }
}
