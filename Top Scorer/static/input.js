$(function () {
  var initButton = $("#init-button"),
    app = $("#app"),
    form = $("#form"),
    searchInput = $("#search-input"),
    rcBtns = $("#r-btn, #c-btn"),
    submitButton = $("#submit-button");

  function initApp() {
    app.toggleClass("active");

    if (form.hasClass("active")) form.removeClass("active");
    else {
      setTimeout(function () {
        form.addClass("active");
      }, 40);
      setTimeout(function () {
        searchInput.focus();
      }, 600);
    }
  }

  function checkInput() {
    if ($(this).val().trim().length > 0) {
      rcBtns.addClass("change");
      submitButton.addClass("active");
    } else {
      rcBtns.removeClass("change");
      submitButton.removeClass("active");
    }
  }

  initButton.on("click", initApp);
  searchInput.on("keyup", checkInput);
});

