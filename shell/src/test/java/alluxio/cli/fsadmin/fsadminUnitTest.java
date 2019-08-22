package alluxio.cli.fsadmin;

import alluxio.cli.Command;
import alluxio.cli.CommandReader;
import alluxio.cli.fs.FileSystemShellUtils;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.ServerConfiguration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.io.Closer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Map;

public class fsadminUnitTest {
    private static Map<String, Command> commands;

    @Before
    public void loadFsadminCommands(){
        commands = new FileSystemAdminShell(ServerConfiguration.global()).loadCommands();
    }

    @Test
    public void checkDocs() throws IOException {
        for(Map.Entry<String, Command> cmd : commands.entrySet()){
            Command c = cmd.getValue();
            Assert.assertNotNull(c.getCommandName());
            Assert.assertNotNull(c.getUsage());
            Assert.assertNotNull(c.getDescription());
            Assert.assertNotNull(c.getExample());
        }
    }
}
