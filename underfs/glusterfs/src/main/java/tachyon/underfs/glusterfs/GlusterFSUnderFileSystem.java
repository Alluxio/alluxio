package tachyon.underfs.glusterfs;

import org.apache.hadoop.conf.Configuration;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.underfs.hdfs.HdfsUnderFileSystem;

public class GlusterFSUnderFileSystem extends HdfsUnderFileSystem {

  /**
   * Constant for the Gluster FS URI scheme
   */
  public static final String SCHEME = "glusterfs://";

  public GlusterFSUnderFileSystem(String fsDefaultName, TachyonConf tachyonConf, Object conf) {
    super(fsDefaultName, tachyonConf, conf);
  }

  @Override
  protected void prepareConfiguration(String path, TachyonConf tachyonConf, Configuration config) {
    if (path.startsWith(SCHEME)) {
      // Configure for Gluster FS
      config.set("fs.glusterfs.impl", tachyonConf.get(Constants.UNDERFS_GLUSTERFS_IMPL,
          "org.apache.hadoop.fs.glusterfs.GlusterFileSystem"));
      config.set("mapred.system.dir",
          tachyonConf.get(Constants.UNDERFS_GLUSTERFS_MR_DIR, "glusterfs:///mapred/system"));
      config.set("fs.glusterfs.volumes",
          tachyonConf.get(Constants.UNDERFS_GLUSTERFS_VOLUMES, null));
      config
          .set(
              "fs.glusterfs.volume.fuse."
                  + tachyonConf.get(Constants.UNDERFS_GLUSTERFS_VOLUMES, null),
              tachyonConf.get(Constants.UNDERFS_GLUSTERFS_MOUNTS, null));
    } else {
      // If not Gluster FS fall back to default HDFS behaviour
      super.prepareConfiguration(path, tachyonConf, config);
    }
  }

}
