jQuery(document).ready(function($) {

  $('img').addClass('img-responsive');

  //Bootstrap Navbar Click toggle Fix for hover effect
  $('#navbar-toggle').click(function(){
    $(this).toggleClass('open');
  });

  $('.dropdown').click(function(event){
     event.stopPropagation();
   });

});
