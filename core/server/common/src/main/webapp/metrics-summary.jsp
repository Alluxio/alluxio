<%--

    The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
    (the "License"). You may not use this work except in compliance with the License, which is
    available at www.apache.org/licenses/LICENSE-2.0

    This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
    either express or implied, as more fully set forth in the License.

    See the NOTICE file distributed with this work for information regarding copyright ownership.

--%>
<%@ page import="java.util.*" %>
<%@ page import="alluxio.web.*" %>

<div class="row-fluid">
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
                <th>Directories Created</th>
                <th><%= request.getAttribute("DirectoriesCreated") %></th>
                <th>File Block Infos Got</th>
                <th><%= request.getAttribute("FileBlockInfosGot") %></th>
              </tr>
              <tr>
                <th>File Infos Got</th>
                <th><%= request.getAttribute("FileInfosGot") %></th>
                <th>Files Completed</th>
                <th><%= request.getAttribute("FilesCompleted") %></th>
              </tr>
              <tr>
                <th>Files Created</th>
                <th><%= request.getAttribute("FilesCreated") %></th>
                <th>Files Freed</th>
                <th><%= request.getAttribute("FilesFreed") %>
                </th>
              </tr>
              <tr>
                <th>Files Persisted</th>
                <th><%= request.getAttribute("FilesPersisted") %></th>
                <th>Files Pinned</th>
                <th><%= request.getAttribute("FilesPinned") %></th>
              </tr>
              <tr>
                <th>New Blocks Got</th>
                <th><%= request.getAttribute("NewBlocksGot") %></th>
                <th>Paths Deleted</th>
                <th><%= request.getAttribute("PathsDeleted") %></th>
              </tr>
              <tr>
                <th>Paths Mounted</th>
                <th><%= request.getAttribute("PathsMounted") %></th>
                <th>Paths Renamed</th>
                <th><%= request.getAttribute("PathsRenamed") %></th>
              </tr>
              <tr>
                <th>Paths Unmounted</th>
                <th><%= request.getAttribute("PathsUnmounted") %></th>
                <th></th>
                <th></th>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>
  </div>
</div>
<div class="row-fluid">
  <div class="accordion span14" id="accordion5">
    <div class="accordion-group">
      <div class="accordion-heading">
        <a class="accordion-toggle" data-toggle="collapse" data-parent="#accordion5" href="#data5">
          <h4>RPC Invocations</h4>
        </a>
      </div>
      <div id="data5" class="accordion-body collapse in">
        <div class="accordion-inner">
          <table class="table">
            <tbody>
              <tr>
                <th>CompleteFile Operations</th>
                <th><%= request.getAttribute("CompleteFileOps") %></th>
                <th>CreateDirectory Operations</th>
                <th><%= request.getAttribute("CreateDirectoryOps") %></th>
              </tr>
              <tr>
                <th>CreateFile Operations</th>
                <th><%= request.getAttribute("CreateFileOps") %></th>
                <th>DeletePath Operations</th>
                <th><%= request.getAttribute("DeletePathOps") %></th>
              </tr>
              <tr>
                <th>FreeFile Operations</th>
                <th><%= request.getAttribute("FreeFileOps") %></th>
                <th>GetFileBlockInfo Operations</th>
                <th><%= request.getAttribute("GetFileBlockInfoOps") %></th>
              </tr>
              <tr>
                <th>GetFileInfo Operations</th>
                <th><%= request.getAttribute("GetFileInfoOps") %></th>
                <th>GetNewBlock Operations</th>
                <th><%= request.getAttribute("GetNewBlockOps") %></th>
              </tr>
              <tr>
                <th>Mount Operations</th>
                <th><%= request.getAttribute("MountOps") %></th>
                <th>RenamePath Operations</th>
                <th><%= request.getAttribute("RenamePathOps") %></th>
              </tr>
              <tr>
                <th>SetAttribute Operations</th>
                <th><%= request.getAttribute("SetAttributeOps") %></th>
                <th>Unmount Operations</th>
                <th><%= request.getAttribute("UnmountOps") %></th>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>
  </div>
</div>
