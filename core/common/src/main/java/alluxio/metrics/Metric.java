package alluxio.metrics;

import com.google.common.base.Preconditions;

import java.io.Serializable;

public final class Metric implements Serializable {
  private static final long serialVersionUID = -2236393414222298333L;
  private final String mInstance;
  private final String mHostname;
  private final String mName;
  private final Object mValue;

  public Metric(String instance, String host, String name, Object value) {
    mInstance = instance;
    mHostname = host;
    mName = name;
    mValue = value;
  }

  public String getInstance() {
    return mInstance;
  }

  public String getHostname() {
    return mHostname;
  }

  public String getName() {
    return mName;
  }

  public Object getValue() {
    return mValue;
  }

  public String getFullMetricName() {
    StringBuilder sb = new StringBuilder();
    sb.append(mInstance).append('.');
    if (mHostname != null) {
      sb.append(mHostname).append('.');
    }
    sb.append(mName);
    return sb.toString();
  }

  public alluxio.thrift.Metric toThrift() {
    alluxio.thrift.Metric metric=new alluxio.thrift.Metric();
    metric.setInstance(mInstance);
    metric.setHostname(mHostname);
    metric.setName(mName);
    if (!(mValue instanceof Integer) && !(mValue instanceof Long)) {
      throw new UnsupportedOperationException(
          "The value " + mValue + " is not integer, neither long");
    }
    metric.setValue((Long) mValue);
    return metric;
  }

  public static Metric from(String fullName, Object value) {
    String[] pieces = fullName.split("\\.");
    Preconditions.checkArgument(pieces.length > 1, "Incorrect metrics name: %s.", fullName);

    String hostname = null;
    // Master or cluster metrics don't have hostname included.
    if (!pieces[0].equals(MetricsSystem.MASTER_INSTANCE)
        && !pieces[0].equals(MetricsSystem.CLUSTER)) {
      hostname = pieces[1];
    }
    String instance = pieces[0];
    String name = MetricsSystem.stripInstanceAndHost(fullName);
    return new Metric(instance, hostname, name, value);
  }

  public static Metric from(alluxio.thrift.Metric metric) {
    return new Metric(metric.getInstance(), metric.getHostname(), metric.getName(),
        metric.getValue());
  }


}
