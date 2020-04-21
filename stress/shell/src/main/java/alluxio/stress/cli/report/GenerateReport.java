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

package alluxio.stress.cli.report;

import alluxio.stress.GraphGenerator;
import alluxio.stress.JsonSerializable;
import alluxio.stress.Summary;
import alluxio.stress.graph.Graph;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Generates a report from summaries.
 */
public class GenerateReport {
  private static final Logger LOG = LoggerFactory.getLogger(GenerateReport.class);

  @ParametersDelegate
  private Parameters mParameters = new Parameters();

  @com.beust.jcommander.Parameters(separators = "=")
  private static class Parameters {
    @Parameter(names = "--input",
        description = "The input json files of the results. Can be repeated", required = true)
    private List<String> mInputs;

    @Parameter(names = "--output-dir", description = "The output directory", required = true)
    private String mOutputDir;
  }

  /**
   * @param args the command-line args
   */
  public static void main(String[] args) {
    new GenerateReport().run(args);
  }

  GenerateReport() {
  }

  private void run(String[] args) {
    JCommander jc = new JCommander(this);
    jc.setProgramName(this.getClass().getSimpleName());
    try {
      jc.parse(args);
    } catch (Exception e) {
      jc.usage();
      throw e;
    }

    List<Summary> inputs =
        mParameters.mInputs.stream().map(f -> {
          try {
            return readJson(f);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }).collect(Collectors.toList());

    String className = null;
    for (Summary summary : inputs) {
      if (className == null) {
        className = summary.getClassName();
      }
      if (!className.equals(summary.getClassName())) {
        throw new RuntimeException(
            "Mismatched input result types: " + className + " , " + summary.getClassName());
      }
    }

    File outputDir = new File(mParameters.mOutputDir);
    outputDir.mkdirs();

    GraphGenerator graphGenerator = inputs.get(0).graphGenerator();

    List<Graph> graphs = graphGenerator.generate(inputs);

    try (PrintWriter writer = new PrintWriter(
        new FileWriter(Paths.get(outputDir.getAbsolutePath(), "index.html").toString()))) {

      writer.println("<!DOCTYPE html>");
      writer.println("<head>");
      writer.println("<script src=\"https://cdn.jsdelivr.net/npm/vega@5\"></script>");
      writer.println("<script src=\"https://cdn.jsdelivr.net/npm/vega-lite@4\"></script>");
      writer.println("<script src=\"https://cdn.jsdelivr.net/npm/vega-embed@6\"></script>");
      writer.println("</head>");
      writer.println("<body>");

      for (int i = 0; i < graphs.size(); i++) {
        writer.println("<div id=\"graph" + i + "\"></div>");
      }
      writer.println("<script>");

      for (int i = 0; i < graphs.size(); i++) {
        writer.println(String.format("const json%d = `", i));
        writer.println(graphs.get(i).toJson());
        writer.println("`;");
        writer.println(String.format("vegaEmbed(\"#graph%d\", JSON.parse(json%d))", i, i));
      }

      writer.println("</script>");
      writer.println("</body>");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Summary readJson(String filename) throws IOException, ClassNotFoundException {
    return JsonSerializable
        .fromJson(new String(Files.readAllBytes(Paths.get(filename))), new Summary[0]);
  }
}
