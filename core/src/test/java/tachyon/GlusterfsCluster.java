package tachyon;

import java.io.File;
import java.io.IOException;

import tachyon.conf.MasterConf;

public class GlusterfsCluster extends UnderFileSystemCluster {

  public GlusterfsCluster(String baseDir) {
    super(baseDir);
    mMount = MasterConf.get().getProperty("tachyon.underfs.glusterfs.mounts");
    mVolume = MasterConf.get().getProperty("tachyon.underfs.glusterfs.volumes");
    if (mMount != null && !mMount.equals("") && mVolume != null && !mVolume.equals("")) {
      if (new File(mMount).exists()) {
        mUseGfs = true;
      }
    }

  }

  @Override
  public String getUnderFilesystemAddress() {
    return "glusterfs:///tachyon_test";
  }

  @Override
  public boolean isStarted() {
    return true;
  }

  @Override
  public void shutdown() throws IOException {}

  @Override
  public void start() throws IOException {}
}
