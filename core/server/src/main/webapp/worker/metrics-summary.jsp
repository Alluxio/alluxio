<%@ page import="java.util.*" %>
<%@ page import="tachyon.web.*" %>
<%@ page import="tachyon.web.*" %>

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
