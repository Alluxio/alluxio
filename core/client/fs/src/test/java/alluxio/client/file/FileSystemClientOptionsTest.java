package alluxio.client.file;

import alluxio.Constants;
import alluxio.grpc.*;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the {@link FileSystemClientOptions}.
 */
public class FileSystemClientOptionsTest {
    @Test
    public void commonOptionsDefaults() {
        FileSystemMasterCommonPOptions options =  FileSystemClientOptions.getCommonOptions();
        Assert.assertNotNull(options);
        Assert.assertEquals(Constants.NO_TTL, options.getTtl());
        Assert.assertEquals(alluxio.grpc.TtlAction.DELETE, options.getTtlAction());
    }

    @Test
    public void getStatusOptionsDefaults() {
        GetStatusPOptions options =  FileSystemClientOptions.getGetStatusOptions();
        Assert.assertNotNull(options);
        Assert.assertEquals(LoadMetadataPType.ONCE, options.getLoadMetadataType());
    }

    @Test
    public void listStatusOptionsDefaults() {
        ListStatusPOptions options =  FileSystemClientOptions.getListStatusOptions();
        Assert.assertNotNull(options);
        Assert.assertEquals(LoadMetadataPType.ONCE, options.getLoadMetadataType());
        Assert.assertEquals(false, options.getRecursive());
    }

    @Test
    public void loadMetadataOptionsDefaults() {
        LoadMetadataPOptions options =  FileSystemClientOptions.getLoadMetadataOptions();
        Assert.assertNotNull(options);
        Assert.assertFalse(options.getRecursive());
    }
}