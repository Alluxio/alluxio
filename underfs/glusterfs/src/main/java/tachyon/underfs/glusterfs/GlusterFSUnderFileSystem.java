package tachyon.underfs.glusterfs;

import org.apache.hadoop.conf.Configuration;

import tachyon.conf.CommonConf;
import tachyon.underfs.hdfs.HdfsUnderFileSystem;

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
      super.prepareConfiguration(path, config);
    }
  }

}
