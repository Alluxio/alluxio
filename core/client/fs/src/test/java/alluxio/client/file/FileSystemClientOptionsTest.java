package alluxio.client.file;

import alluxio.Constants;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.LoadMetadataPType;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the {@link FileSystemClientOptions}.
 */
public class FileSystemClientOptionsTest {
    @Test
    public void commonOptionDefaults() {
        FileSystemMasterCommonPOptions options =  FileSystemClientOptions.getCommonOptions();
        Assert.assertNotNull(options);
        Assert.assertEquals(Constants.NO_TTL, options.getTtl());
        Assert.assertEquals(alluxio.grpc.TtlAction.DELETE, options.getTtlAction());
    }

    @Test
    public void getStatusOptionDefaults() {
        GetStatusPOptions options =  FileSystemClientOptions.getGetStatusOptions();
        Assert.assertNotNull(options);
        Assert.assertEquals(LoadMetadataPType.ONCE, options.getLoadMetadataType());
    }

    @Test
    public void listStatusOptionDefaults() {
        ListStatusPOptions options =  FileSystemClientOptions.getListStatusOptions();
        Assert.assertNotNull(options);
        Assert.assertEquals(LoadMetadataPType.ONCE, options.getLoadMetadataType());
        Assert.assertEquals(false, options.getRecursive());
    }
}