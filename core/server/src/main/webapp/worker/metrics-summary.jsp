<%--
~ The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
~ (the “License”). You may not use this work except in compliance with the License, which is
~ available at www.apache.org/licenses/LICENSE-2.0
~
~ This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
~ either express or implied, as more fully set forth in the License.
~
~ See the NOTICE file distributed with this work for information regarding copyright ownership.
--%>

<%@ page import="java.util.*" %>
<%@ page import="alluxio.web.*" %>
<%@ page import="alluxio.web.*" %>

<div class="accordion span14" id="accordion4">
  <div class="accordion-group">
    <div class="accordion-heading">
      <a class="accordion-toggle" data-toggle="collapse" data-parent="#accordion4" href="#data4">
        <h4>Logical Operations</h4>
      </a>
    </div>

    <div id="data4" class="accordion-body collapse in">
      <div class="accordion-inner">
        <table class="table">
          <tbody>
            <tr>
              <th>Blocks Accessed</th>
              <th><%= request.getAttribute("worker.BlocksAccessed") %></th>
              <th>Blocks Cached</th>
              <th><%= request.getAttribute("worker.BlocksCached") %></th>
            </tr>
            <tr>
              <th>Blocks Canceled</th>
              <th><%= request.getAttribute("worker.BlocksCanceled") %></th>
              <th>Blocks Deleted</th>
              <th><%= request.getAttribute("worker.BlocksDeleted") %></th>
            </tr>
            <tr>
              <th>Blocks Evicted</th>
              <th><%= request.getAttribute("worker.BlocksEvicted") %></th>
              <th>Blocks Promoted</th>
              <th><%= request.getAttribute("worker.BlocksPromoted") %></th>
            </tr>
            <tr>
              <th>Blocks Read Locally</th>
              <th><%= request.getAttribute("worker.BlocksReadLocal") %></th>
              <th>Blocks Read Remotely</th>
              <th><%= request.getAttribute("worker.BlocksReadRemote") %></th>
            </tr>
            <tr>
              <th>Blocks Written Locally</th>
              <th><%= request.getAttribute("worker.BlocksWrittenLocal") %></th>
              <th>Bytes Read Locally</th>
              <th><%= request.getAttribute("worker.BytesReadLocal") %></th>
            </tr>
            <tr>
              <th>Bytes Read Remotely</th>
              <th><%= request.getAttribute("worker.BytesReadRemote") %></th>
              <th>Underfs Bytes read</th>
              <th><%= request.getAttribute("worker.BytesReadUfs") %></th>
            </tr>
            <tr>
              <th>Bytes Written Locally</th>
              <th><%= request.getAttribute("worker.BytesWrittenLocal") %></th>
              <th>Underfs Bytes Written</th>
              <th><%= request.getAttribute("worker.BytesWrittenUfs") %></th>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>
</div>
