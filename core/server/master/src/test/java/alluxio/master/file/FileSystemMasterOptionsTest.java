package alluxio.master.file;

import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.wire.LoadMetadataType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the {@link alluxio.master.file.FileSystemMasterOptions}.
 */
public class FileSystemMasterOptionsTest {

    private FileSystemMasterOptions mMasterOptions;

    @Before
    public void init() {
        mMasterOptions = new DefaultFileSystemMasterOptions();
    }

    @Test
    public void listStatusOptionDefaults() {
        ListStatusPOptions options =  mMasterOptions.getListStatusOptions();
        Assert.assertNotNull(options);
        Assert.assertEquals(LoadMetadataPType.ONCE, options.getLoadMetadataType());
        Assert.assertEquals(false, options.getRecursive());
    }
}