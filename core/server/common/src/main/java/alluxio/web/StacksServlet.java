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

package alluxio.web;

import alluxio.util.ThreadUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintStream;

/**
 * Stacks servlet to display the stacks of this process.
 */
public class StacksServlet extends HttpServlet {
  private static final long serialVersionUID = 4190506509049119126L;
  private static final Logger LOG = LoggerFactory.getLogger(StacksServlet.class);

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    resp.setContentType("text/plain; charset=UTF-8");
    try (PrintStream out = new PrintStream(
        resp.getOutputStream(), false, "UTF-8")) {
      ThreadUtils.printThreadInfo(out, "");
    }
    ThreadUtils.logThreadInfo(LOG, "jsp requested", 1);
  }
}
