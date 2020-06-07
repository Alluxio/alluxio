package alluxio.cli.validation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * An enum standing for a UFS type.
 * */
@Retention(RetentionPolicy.RUNTIME)
public @interface ApplicableUfsType {
  /**
   * Gets the value.
   *
   * @return the UFS type
   * */
  Type value() default Type.ALL;

  /**
   * UFS types.
   * */
  enum Type {
    ALL,
    HDFS,
    OBJECT
  }
}
