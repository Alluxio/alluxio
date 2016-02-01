<%@ page import="java.util.*" %>
<%@ page import="tachyon.web.*" %>
<%@ page import="tachyon.web.*" %>

<script type="text/javascript">
  jsonString = ${param.operationMetrics};
</script>

<div class="accordion span14" id="accordion4">
  <div class="accordion-group">
    <div class="accordion-heading">
      <a class="accordion-toggle" data-toggle="collapse" data-parent="#accordion4" href="#data4">
        <h4>Operations</h4>
      </a>
    </div>

    <div id="data4" class="accordion-body collapse in">
      <div class="accordion-inner">

        <table class="table table-condensed">
          <thead>
            <th></th>
            <th></th>
          </thead>
          <tbody>
            <tr>
              <th>Blocks Accessed</th>
              <th>Blocks Cached</th>
            </tr>
            <tr>
              <th>
                <div id="BlocksAccessed">
                  <script type="text/javascript">
                    linearGraph("#BlocksAccessed", jsonString["worker.BlocksAccessed"]);
                  </script>
                </div>

              </th>
              <th>
                <div id="BlocksCached">
                  <script type="text/javascript">
                    linearGraph("#BlocksCached", jsonString["worker.BlocksCached"]);
                  </script>
                </div>
              </th>
            </tr>
            <tr>
              <th>Blocks Canceled</th>
              <th>Blocks Deleted</th>
            </tr>
            <tr>
              <th>
                <div id="BlocksCanceled">
                  <script type="text/javascript">
                    linearGraph("#BlocksCanceled", jsonString["worker.BlocksCanceled"]);
                  </script>
                </div>

              </th>
              <th>
                <div id="BlocksDeleted">
                  <script type="text/javascript">
                    linearGraph("#BlocksDeleted", jsonString["worker.BlocksDeleted"]);
                  </script>
                </div>
              </th>
            </tr>
            <tr>
              <th>Blocks Evicted</th>
              <th>Blocks Promoted</th>
            </tr>
            <tr>
              <th>
                <div id="BlocksEvicted">
                  <script type="text/javascript">
                    linearGraph("#BlocksEvicted", jsonString["worker.BlocksEvicted"]);
                  </script>
                </div>
              </th>
              <th>
                <div id="BlocksPromoted">
                  <script type="text/javascript">
                    linearGraph("#BlocksPromoted", jsonString["worker.BlocksPromoted"]);
                  </script>
                </div>
              </th>
            </tr>
            <tr>
              <th>Blocks Read Locally</th>
              <th>Blocks Read Remotely</th>
            </tr>
            <tr>
              <th>
                <div id="BlocksReadLocal">
                  <script type="text/javascript">
                    linearGraph("#BlocksReadLocal", jsonString["worker.BlocksReadLocal"]);
                  </script>
                </div>
              </th>
              <th>
                <div id="BlocksReadRemote">
                  <script type="text/javascript">
                    linearGraph("#BlocksReadRemote", jsonString["worker.BlocksReadRemote"]);
                  </script>
                </div>
              </th>
            </tr>
            <tr>
              <th>Blocks Written Locally</th>
              <th>Bytes Read Locally</th>
            </tr>
            <tr>
              <th>
                <div id="BlocksWrittenLocal">
                  <script type="text/javascript">
                    linearGraph("#BlocksWrittenLocal", jsonString["worker.BlocksWrittenLocal"]);
                  </script>
                </div>
              </th>
              <th>
                <div id="BytesReadLocal">
                  <script type="text/javascript">
                    linearGraph("#BytesReadLocal", jsonString["worker.BytesReadLocal"]);
                  </script>
                </div>
              </th>
            </tr>

            <tr>
              <th>Bytes Read Remotely</th>
              <th>Underfs Bytes read</th>
            </tr>
            <tr>
              <th>
                <div id="BytesReadRemote">
                  <script type="text/javascript">
                    linearGraph("#BytesReadRemote", jsonString["worker.BytesReadRemote"]);
                  </script>
                </div>
              </th>
              <th>
                <div id="BytesReadUfs">
                  <script type="text/javascript">
                    linearGraph("#BytesReadUfs", jsonString["worker.BytesReadUfs"]);
                  </script>
                </div>
              </th>
            </tr>
            <tr>
              <th>Bytes Written Locally</th>
              <th>Underfs Bytes Written</th>
            </tr>
            <tr>
              <th>
                <div id="BytesWrittenLocal">
                  <script type="text/javascript">
                    linearGraph("#BytesWrittenLocal", jsonString["worker.BytesWrittenLocal"]);
                  </script>
                </div>
              </th>
              <th>
                <div id="BytesWrittenUfs">
                  <script type="text/javascript">
                    linearGraph("#BytesWrittenUfs", jsonString["worker.BytesWrittenUfs"]);
                  </script>
                </div>
              </th>
            </tr>
          </tbody>
        </table>

      </div>
    </div>
  </div>
</div>