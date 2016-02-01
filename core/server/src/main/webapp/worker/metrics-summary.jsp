<%@ page import="java.util.*" %>
<%@ page import="tachyon.web.*" %>
<%@ page import="tachyon.web.*" %>

<div class="accordion span14" id="accordion4">
  <div class="accordion-group">
    <div class="accordion-heading">
      <a class="accordion-toggle" data-toggle="collapse" data-parent="#accordion4" href="#data4">
        <h4>Operations</h4>
      </a>
    </div>

    <div id="data4" class="accordion-body collapse in">
      <div class="accordion-inner">
        <table class="table table-condensed table-centered">
          <thead>
            <th></th>
            <th></th>
          </thead>
          <tbody>
            <tr>
              <th>Blocks Accessed</th>
              <th>Blocks Cached</th>
              <th>Blocks Canceled</th>
            </tr>
            <tr>
              <td>
                <div id="BlocksAccessed" class="huge">
                    <%= request.getAttribute("worker.BlocksAccessed") %>
                </div>
              </td>
              <td>
                <div id="BlocksCached" class="huge">
                    <%= request.getAttribute("worker.BlocksCached") %>
                </div>
              </td>
              <td>
                <div id="BlocksCanceled" class="huge">
                    <%= request.getAttribute("worker.BlocksCanceled") %>
                </div>
              </td>
            </tr>
            <tr>
              <th>Blocks Deleted</th>
              <th>Blocks Evicted</th>
              <th>Blocks Promoted</th>
            </tr>
            <tr>
              <td>
                <div id="BlocksDeleted" class="huge">
                    <%= request.getAttribute("worker.BlocksDeleted") %>
                </div>
              </td>
              <td>
                <div id="BlocksEvicted" class="huge">
                    <%= request.getAttribute("worker.BlocksEvicted") %>
                </div>
              </td>
              <td>
                <div id="BlocksPromoted" class="huge">
                    <%= request.getAttribute("worker.BlocksPromoted") %>
                </div>
              </td>
            </tr>
            <tr>
              <th>Blocks Read Locally</th>
              <th>Blocks Read Remotely</th>
              <th>Blocks Written Locally</th>
            </tr>
            <tr>
              <td>
                <div id="BlocksReadLocal" class="huge">
                    <%= request.getAttribute("worker.BlocksReadLocal") %>
                </div>
              </td>
              <td>
                <div id="BlocksReadRemote" class="huge">
                    <%= request.getAttribute("worker.BlocksReadRemote") %>                  
                </div>
              </td>
              <td>
                <div id="BlocksWrittenLocal" class="huge">                  
                    <%= request.getAttribute("worker.BlocksWrittenLocal") %>
                </div>
              </td>
            </tr>
            <tr>
              <th>Bytes Read Locally</th>
              <th>Bytes Read Remotely</th>
              <th>Underfs Bytes read</th>
            </tr>
            <tr>
              <td>
                <div id="BytesReadLocal" class="huge">
                    <%= request.getAttribute("worker.BytesReadLocal") %>
                </div>
              </td>
              <td>
                <div id="BytesReadRemote" class="huge">
                    <%= request.getAttribute("worker.BytesReadRemote") %>
                </div>
              </td>
              <td>
                <div id="BytesReadUfs" class="huge">
                    <%= request.getAttribute("worker.BytesReadUfs") %>
                </div>
              </td>
            </tr>
            <tr>
              <th>Bytes Written Locally</th>
              <th>Underfs Bytes Written</th>
            </tr>
            <tr>
              <td>
                <div id="BytesWrittenLocal" class="huge">
                    <%= request.getAttribute("worker.BytesWrittenLocal") %>
                </div>
              </td>
              <td>
                <div id="BytesWrittenUfs" class="huge">
                    <%= request.getAttribute("worker.BytesWrittenUfs") %>
                </div>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>
</div>