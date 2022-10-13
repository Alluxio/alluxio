/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.stress.cli.worker;

import alluxio.AlluxioURI;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.exception.AlluxioException;
import alluxio.hadoop.HadoopConfigurationUtils;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.stress.cli.StressMasterBench;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import java.net.URI;
import java.util.List;
import java.util.ArrayList;
import java.io.PrintStream;
import java.io.IOException;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class create file tree for load metadata
 */
public class CreateTestTree {

    private static final Logger LOG = LoggerFactory.getLogger(CreateTestTree.class);

    @Parameter(names = { "-depth" }, description = "File tree depth")
    int depth;
    @Parameter(names = { "-width" }, description = "File tree width")
    int width;
    @Parameter(names = { "-childFileCount" }, description = "File count for each node")
    static int childFileCount = 0;
    @Parameter(names = { "-threads" }, description = "Number of file tree")
    int threads;

    static FileContext fc;
    static alluxio.client.file.FileSystem fs;

    public static void main(String ... args)  {
        try { // initialize file system handle
            fc = FileContext.getFileContext();
        } catch (IOException ioe) {
            System.err.println("Can not initialize the file system: " +
                    ioe.getLocalizedMessage());
            return ;
        }
        Configuration hdfsConf = new Configuration();
        hdfsConf.set(PropertyKey.Name.USER_FILE_WRITE_TYPE_DEFAULT, "MUST_CACHE");
        InstancedConfiguration alluxioProperties = alluxio.conf.Configuration.copyGlobal();
        alluxioProperties.merge(HadoopConfigurationUtils.getConfigurationFromHadoop(hdfsConf), Source.RUNTIME);
        fs = alluxio.client.file.FileSystem.Factory.create(alluxioProperties);
        CreateTestTree tree = new CreateTestTree();
        JCommander.newBuilder()
                .addObject(tree)
                .build()
                .parse();
        try {
            tree.run();
        } catch (Exception e) {
            LOG.warn("Exception during creating test file tree", e);
        }
    }

    private void init() {
        try { // initialize file system handle
            fc = FileContext.getFileContext();
        } catch (IOException ioe) {
            System.err.println("Can not initialize the file system: " +
                    ioe.getLocalizedMessage());
            return ;
        }


    }

    public void run() throws IOException, AlluxioException {
        genDirStructure();
        output();
    }

    /** In memory representation of a directory */
    private static class INode {
        private String name;
        private List<INode> children = new ArrayList<INode>();

        /** Constructor */
        private INode(String name) {
            this.name = name;
        }

        /** Add a child (subdir/file) */
        private void addChild(INode child) {
            children.add(child);
        }

        /** Output the subtree rooted at the current node.
         * Only the leaves are printed.
         */
        private void output(String prefix) throws IOException, AlluxioException {
            prefix = prefix==null?name:prefix+"/"+name;

            if (children.isEmpty()) {
                System.out.println(prefix);
                // fc.mkdir(new Path(prefix), FileContext.DEFAULT_PERM, true);
                fs.createDirectory(new AlluxioURI(prefix.toString()), CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
                for (int i = 0; i < childFileCount; i++) {
                    System.out.println(prefix + "/" + i + ".txt");
                    fs.createFile(new AlluxioURI((prefix + "/" + i + ".txt").toString()), CreateFilePOptions.newBuilder().setRecursive(true).build()).close();
                }
            } else {
                System.out.println(prefix);
                fs.createDirectory(new AlluxioURI(prefix.toString()), CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
                for (int i = 0; i < childFileCount; i++) {
                    System.out.println(prefix + "/" + i + ".txt");
                    fs.createFile(new AlluxioURI((prefix + "/" + i + ".txt").toString()), CreateFilePOptions.newBuilder().setRecursive(true).build()).close();
                }
                for (INode child : children) {
                    child.output(prefix);
                }
            }
        }

        /** Output the files in the subtree rooted at this node */
        protected void outputFiles(PrintStream out, String prefix) {
            prefix = prefix==null?name:prefix+"/"+name;
            for (INode child : children) {
                child.outputFiles(out, prefix);
            }
        }

        /** Add all the leaves in the subtree to the input list */
        private void getLeaves(List<INode> leaves) {
            if (children.isEmpty()) {
                leaves.add(this);
            } else {
                for (INode child : children) {
                    child.getLeaves(leaves);
                }
            }
        }
    }

    private INode genDirStructure(String rootName, int Depth) {
        INode root = new INode(rootName);

        if (Depth>0) {
            Depth--;
            for (int i=0; i<width; i++) {
                INode child = genDirStructure("dir"+i, Depth);
                root.addChild(child);
            }
        }
        return root;
    }

    private void genDirStructure() {
        System.out.println("genDir");
        root = new INode("/metadata_test");
        for (int i = 0; i < threads; i++) {
            INode thread_root = genDirStructure("thread" + i, depth);
            root.addChild(thread_root);
        }
    }

    private INode root;

    /** Output directory structure to a file, each line of the file
     * contains the directory name. Only empty directory names are printed. */
    private void output() throws IOException, AlluxioException {
        System.out.println("Printing");
        root.output(null);
    }

}
