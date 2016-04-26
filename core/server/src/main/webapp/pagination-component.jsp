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

<!-- pagination component -->
<div class="pagination pagination-centered">
  <ul id="paginationUl">
  </ul>
</div>

<!-- view setting panel -->
<div class="accordion">
  <div class="accordion-group">
    <div class="accordion-heading">
      <a class="accordion-toggle" data-toggle="collapse" href="#viewSettings">
        <h4>View Settings</h4>
      </a>
      <div id="viewSettings" class="accordion-body collapse">
        <div class="accordion-inner">
          <table class="table">
            <tbody>
              <tr>
                <th>Number of items per page:</th>
                <th><input id="nFilePerPage" type="text" placeholder="default = 20"></th>
              </tr>
              <tr>
                <th>Maximum number of pages to show in pagination component:</th>
                <th><input id="nMaxPageShown" type="text" placeholder="default = 10"></th>
              </tr>
            </tbody>
          </table>
          <button class="btn" id="updateView">Update</button>
        </div>
      </div>
    </div>
  </div>
</div>