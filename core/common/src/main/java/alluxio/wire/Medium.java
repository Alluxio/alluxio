package alluxio.wire;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

public enum Medium {
  MEM,
  SSD,
  HDD;

  public static final Set<Medium> EMPTY_SET =
      Collections.unmodifiableSet(EnumSet.noneOf(Medium.class));

  public static Set<Medium> fromValues(Collection<String> values) {
    Set<Medium> mediums;
    if (values.isEmpty()) {
      mediums = Medium.EMPTY_SET;
    } else {
      mediums = EnumSet.noneOf(Medium.class);
      for (String s : values) {
        // TODO(jiacheng): If the value is not defined, IllegalArgumentException
        mediums.add(Medium.valueOf(s));
      }
    }
    return mediums;
  }
}
