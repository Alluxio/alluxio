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

package alluxio.conf;

import alluxio.annotation.PublicApi;

import com.google.common.base.Objects;

/**
 * The source of a configuration property.
 *
 * The natural ordering of this class is not consistent with the {@link #equals(Object)} method.
 */
@PublicApi
public class Source implements Comparable<Source> {
  public static final Source UNKNOWN = new Source(Type.UNKNOWN);
  public static final Source DEFAULT = new Source(Type.DEFAULT);
  public static final Source CLUSTER_DEFAULT = new Source(Type.CLUSTER_DEFAULT);
  public static final Source SYSTEM_PROPERTY = new Source(Type.SYSTEM_PROPERTY);
  public static final Source PATH_DEFAULT = new Source(Type.PATH_DEFAULT);
  public static final Source RUNTIME = new Source(Type.RUNTIME);
  public static final Source MOUNT_OPTION = new Source(Type.MOUNT_OPTION);

  /**
   * Source type.
   */
  public enum Type {
    /**
     * The unknown source which has the lowest priority.
     */
    UNKNOWN,
    /**
     * The default property value from <code>PropertyKey</code> on compile time.
     */
    DEFAULT,
    /**
     * The default property value as loaded from the masters of the cluster.
     */
    CLUSTER_DEFAULT,
    /**
     * The property value is specified in site properties file (alluxio-site.properties).
     */
    SITE_PROPERTY,
    /**
     * The property value is specified with JVM -D options before passed to Alluxio.
     */
    SYSTEM_PROPERTY,
    /**
     * The property value is specified as path level defaults through command line.
     */
    PATH_DEFAULT,
    /**
     * The property value is set by user during runtime (e.g., Configuration.set or through
     * HadoopConf).
     */
    RUNTIME,
    /**
     * The property value is specified as options for a mount point. This source has the highest
     * priority.
     */
    MOUNT_OPTION,
  }

  protected final Type mType;

  private Source(Type type) {
    mType = type;
  }

  /**
   * @return the type of the source
   */
  public Type getType() {
    return mType;
  }

  /**
   * Creates a site property source with the specified filename.
   *
   * @param filename the filename
   * @return the source
   */
  public static Source siteProperty(String filename) {
    return new SitePropertySource(filename);
  }

  @Override
  public int compareTo(Source other) {
    return mType.compareTo(other.mType);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof Source)) {
      return false;
    }

    Source other = (Source) o;

    return compareTo(other) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mType);
  }

  @Override
  public String toString() {
    return mType.name();
  }

  private static final class SitePropertySource extends Source {
    private final String mFilename;

    private SitePropertySource(String filename) {
      super(Type.SITE_PROPERTY);
      mFilename = filename;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (o == null || this.getClass() != o.getClass()) {
        return false;
      }
      SitePropertySource that = (SitePropertySource) o;
      return Objects.equal(mFilename, that.mFilename);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(mFilename);
    }

    @Override
    public String toString() {
      return String.format("%s (%s)", mType, mFilename);
    }
  }
}
