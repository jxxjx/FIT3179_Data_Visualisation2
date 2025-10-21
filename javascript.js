document.addEventListener('DOMContentLoaded', function() {
  var vg_1 = "arrivals_symbol_map.json";
  vegaEmbed("#symbol_map", vg_1).then(function(result) {
  }).catch(console.error);

  var vg_2 = "arrivals_bar_chart.json";
  vegaEmbed("#bar_chart", vg_2).then(function(result) {
  }).catch(console.error);

var vg_3 = "arrivals_scatter_plot.json";
// Clear the div first to prevent duplicate signals
document.getElementById("scatter_plot").innerHTML = '';
vegaEmbed("#scatter_plot", vg_3, {actions: true}).then(function(result) {
  console.log("Scatter plot loaded successfully!");
}).catch(function(error) {
  console.error("Scatter plot error:", error);
  // Show error in the div
  document.getElementById("scatter_plot").innerHTML = '<p style="color:red;">Error: ' + error.message + '</p>';
});

  var vg_4 = "gender_stacked_bar_chart.json";
  vegaEmbed("#gender_stacked_bar_chart", vg_4).then(function(result) {
  }).catch(console.error);

  var vg_5 = "area_chart.json";
  vegaEmbed("#area_chart", vg_5).then(function(result) {
  }).catch(console.error);

  var vg_6 = "stacked_bar.json";
  vegaEmbed("#stacked_bar", vg_6).then(function(result) {
  }).catch(console.error);

  var vg_7 = "donut.json";
  vegaEmbed("#donut", vg_7).then(function(result) {
  }).catch(console.error);

    var vg_8 = "line_chart.json";
  vegaEmbed("#line_chart", vg_8).then(function(result) {
  }).catch(console.error);
});

