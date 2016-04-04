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