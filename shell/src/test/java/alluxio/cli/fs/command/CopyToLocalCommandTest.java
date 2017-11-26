package alluxio.cli.fs.command;

import alluxio.exception.AlluxioException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import sun.tools.jar.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Scanner;

import static org.junit.Assert.*;

public class CopyToLocalCommandTest {
    private ByteArrayOutputStream mOutput=new ByteArrayOutputStream();
    private ByteArrayOutputStream mError=new ByteArrayOutputStream();

    @Before
    public void setupStreams(){
        System.setOut(new PrintStream(mOutput));
        System.setErr(new PrintStream(mError));
    }

    @After
    public void cleanupStream(){
        System.setOut(null);
        System.setErr(null);
    }


    @Test
    public void getCommandName() throws Exception {
        CopyToLocalCommand command=new CopyToLocalCommand(null);
        assert command.getCommandName()=="copyToLocal";

    }

    @Test
    public void getNumOfArgs() throws Exception {
        CopyToLocalCommand command=new CopyToLocalCommand(null);
        int ret=command.getNumOfArgs();
        Assert.assertEquals(ret,2);
    }

    @Test
    public void run() throws Exception {
        
    }


    @Test
    public void getUsage() throws Exception {
        CopyToLocalCommand command=new CopyToLocalCommand(null);
        String ret=command.getUsage();
        String exceptedRet="copyToLocal <src> <localDst>";
        Assert.assertEquals(ret,exceptedRet);
    }

    @Test
    public void getDescription() throws Exception {
        CopyToLocalCommand command=new CopyToLocalCommand(null);
        String ret=command.getDescription();
        String exceptedRet="Copies a file or a directory from the Alluxio filesystem to the local filesystem.";
        Assert.assertEquals(ret,exceptedRet);
    }


}