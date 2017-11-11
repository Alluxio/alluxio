package alluxio.cli.fs.command;

import alluxio.AlluxioURI;
import alluxio.exception.AlluxioException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

public class CheckConsistencyCommandTest {

    private ByteArrayOutputStream mOutput = new ByteArrayOutputStream();
    private ByteArrayOutputStream mError  = new ByteArrayOutputStream();
    private static final Option REPAIR_OPTION =
                    Option.builder("r")
                    .required(false)
                    .hasArg(false)
                    .desc("repair inconsistent files")
                    .build();

    @Before
    public void setupStreams() {
        System.setOut(new PrintStream(mOutput));
        System.setErr(new PrintStream(mError));
    }

    @After
    public void cleanupStreams() {
        System.setOut(null);
        System.setErr(null);
    }

    @Test
    public void testgetNumOfArgs() throws AlluxioException, IOException{
        CheckConsistencyCommand command = new CheckConsistencyCommand(null);
        int numOfArgs = command.getNumOfArgs();
        Assert.assertEquals(1,numOfArgs);
    }

    @Test
    public void testgetOptions() throws AlluxioException, IOException{
        CheckConsistencyCommand command = new CheckConsistencyCommand(null);
        Options opt = command.getOptions();
        Options expect = new Options().addOption(REPAIR_OPTION);
        Assert.assertEquals(expect,opt);
    }

    @Test
    public void testgetCommandName() throws AlluxioException, IOException{
        CheckConsistencyCommand command = new CheckConsistencyCommand(null);
        String commandName = command.getCommandName();
        String expectName = "checkConsistency";
        Assert.assertEquals(expectName,commandName);
    }

    @Test
    public void testgetUsage() throws AlluxioException, IOException{
        CheckConsistencyCommand command = new CheckConsistencyCommand(null);
        String commandUsage = command.getUsage();
        String expect = "checkConsistency [-r] <Alluxio path>";
        Assert.assertEquals(expect,commandUsage);
    }

    @Test
    public void testgetDescription() throws AlluxioException, IOException{
        CheckConsistencyCommand command = new CheckConsistencyCommand(null);
        String commandDesc = command.getDescription();
        String expect = "Checks the consistency of a persisted file or directory in Alluxio. Any files or "
                + "directories which only exist in Alluxio or do not match the metadata of files in the "
                + "under storage will be returned. An administrator should then reconcile the differences."
                + "Specify -r to repair the inconsistent files.";
        Assert.assertEquals(expect,commandDesc);
    }

    @Test
    public void testcheckConsistency() throws AlluxioException, IOException {
        String arg = "/textfile.txt";
        AlluxioURI path = new AlluxioURI(arg);
        CheckConsistencyCommand command = new CheckConsistencyCommand(null);
        CommandLine cl = command.parseAndValidateArgs(arg);
        mOutput.reset();
        int result = command.run(cl);
        String expectOutput = path + " is consistent with the under storage system.";
        Assert.assertEquals(0,result);
        Assert.assertEquals(expectOutput,mOutput.toString());

    }

}
