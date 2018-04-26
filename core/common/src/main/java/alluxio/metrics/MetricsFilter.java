package alluxio.metrics;

import com.google.common.base.Objects;

public class MetricsFilter {
   private final String mInstanceType;
   private final String mName;
   public MetricsFilter(String instanceType, String name){
     mInstanceType = instanceType;
     mName=name;
   }
   public String getInstanceType(){
     return mInstanceType;
   }

   public String getName(){
     return mName;
   }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof MetricsFilter)) {
      return false;
    }
    MetricsFilter filter = (MetricsFilter) obj;
    return Objects.equal(mInstanceType, filter.mInstanceType) && Objects.equal(mName, filter.mName);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mInstanceType, mName);
  }
}
