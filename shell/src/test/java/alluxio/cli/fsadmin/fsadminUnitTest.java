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
        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        objectMapper.findAndRegisterModules();
        URL u;
        for(Map.Entry<String, Command> cmd : commands.entrySet()){
            u = cmd.getValue().getClass().getClassLoader().getResource(String.format("%s.yml", cmd.getValue().getClass().getSimpleName()));
            CommandReader command = objectMapper.readValue(new File(u.getFile()), CommandReader.class);
            Assert.assertNotNull(command.getName());
            Assert.assertNotNull(command.getUsage());
            Assert.assertNotNull(command.getDescription());
            Assert.assertNotNull(command.getExample());
        }
    }
}
