package tachyon.client.kv;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.util.CommonUtils;

public class KVOutputFormat<K, V> extends FileOutputFormat<K, V> {
  protected static class KVRecordWriter<K, V> implements RecordWriter<K, V> {
    private String mPath;

    public KVRecordWriter(String name) {
      mPath = name;
      CommonUtils.runtimeException(mPath + " Very good ====================================");
    }

    @Override
    public void close(Reporter reporter) throws IOException {
      // TODO Auto-generated method stub

    }

    @Override
    public void write(K key, V value) throws IOException {
      // TODO Auto-generated method stub

    }

  }

  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  @Override
  public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf jobConf, String name,
      Progressable progress) throws IOException {
    LOG.info("JobConf ==============================================" + jobConf.toString());
    return new KVRecordWriter<K, V>(name);
  }
}
