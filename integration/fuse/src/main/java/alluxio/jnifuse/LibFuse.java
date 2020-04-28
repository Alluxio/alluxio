package alluxio.jnifuse;

public class LibFuse {

    public native int fuse_main_real(FuseStubFS fs, int argc, String[] argv);
}
