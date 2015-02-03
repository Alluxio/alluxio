package tachyon.underfs.glusterfs;

import org.apache.hadoop.conf.Configuration;

import tachyon.conf.CommonConf;
import tachyon.underfs.hdfs.HdfsUnderFileSystem;

/**
 * A variant of {@link HdfsUnderFileSystem} that instead uses the Gluster FS
 * <p>
 * Currently this implementation simply manages the extra configuration setup necessary to connect
 * to Gluster FS
 * </p>
 *
 */
public class GlusterFSUnderFileSystem extends HdfsUnderFileSystem {

  /**
   * Constant for the Gluster FS URI scheme
   */
  public static final String SCHEME = "glusterfs://";

  public GlusterFSUnderFileSystem(String fsDefaultName, Object conf) {
    super(fsDefaultName, conf);
  }

  @Override
  protected void prepareConfiguration(String path, Configuration config) {
    if (path.startsWith(SCHEME)) {
      // Configure for Gluster FS
      config.set("fs.glusterfs.impl", CommonConf.get().UNDERFS_GLUSTERFS_IMPL);
      config.set("mapred.system.dir", CommonConf.get().UNDERFS_GLUSTERFS_MR_DIR);
      config.set("fs.glusterfs.volumes", CommonConf.get().UNDERFS_GLUSTERFS_VOLUMES);
      config.set("fs.glusterfs.volume.fuse." + CommonConf.get().UNDERFS_GLUSTERFS_VOLUMES,
          CommonConf.get().UNDERFS_GLUSTERFS_MOUNTS);
    } else {
      // If not Gluster FS fall back to default HDFS behaviour
      // This should only happen if someone creates an instance of this directly rather than via the
      // registry and factory which enforces the GlusterFS prefix being present.
      super.prepareConfiguration(path, config);
    }
  }

}
