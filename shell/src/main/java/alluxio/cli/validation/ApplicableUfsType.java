package alluxio.cli.validation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface ApplicableUfsType {
  Type value() default Type.ALL;
  enum Type {
    ALL,
    HDFS,
    OBJECT
  }
}
