function init_latency(resource) {

    var latNames = ["mean", "median", "95th", "99th", "99_9th", "max"];
    var latencies;
    var x = d3.scale.linear().range([0, width]);
    var y = d3.scale.linear().range([height, 0]);
    var xAxis = d3.svg.axis().scale(x).orient("bottom");
    var yAxis = d3.svg.axis().scale(y).orient("left");
    var latColors = d3.scale.category10();

    var line = d3.svg.line()
        .interpolate("basis")
        .x(function(d) { return x(d.elapsed); })
        .y(function(d) { return y(d.value / 1000); }); // convert to ms


    var svg = d3.select("#latencies p.vis").append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    svg.append("text")
        .attr("class", "label")
        .text(resource);

    svg.append("g")
        .attr("class", "lat x axis")
        .attr("transform", "translate(0," + height + ")");

    svg.append("g")
        .attr("class", "lat y axis");

    svg.append("text")
        .attr("text-anchor", "middle")
        .attr("transform", "translate(" + -(margin.left/2) + "," + (height/2) + ")rotate(-90)")
        .attr("class", "label")
        .text("Latency ms");

    var latBoxes = d3.select("#latencies p.control").selectAll("p")
        .data(latNames)
        .enter()
        .append("p")
        .attr("class", "selection")
        .text(function(d) { return d; })
        .append("input")
        .attr("type", "checkbox")
        .attr("name", "lats")
        .attr("value", function(d) { return d; })
        .attr("checked", true);

    var redraw = function() {

        var checked = d3.selectAll("#latencies p.control p input:checked")[0]
            .map(function(c) { return c.value });

        latColors.domain(latNames);
        var selected = latencies.filter(function(o) {
            return checked.indexOf(o.name) != -1;
        });

        x.domain(d3.extent(selected[0].values, function(d) { return d.elapsed; }));
        y.domain([
            d3.min(selected, function(c) {
                return d3.min(c.values, function(v) { return v.value / 1000; });
            }),
            d3.max(selected, function(c) {
                return d3.max(c.values, function(v) { return v.value / 1000; });
            })
        ]);

        var lat = svg.selectAll(".latency")
            .data(selected, function(d) { return d.name; });

        lat.enter().append("path")
            .attr("class", "line latency")
            .attr("d", function(d) { return line(d.values); })
            .style("stroke", function(d) { return latColors(d.name); })

        var latUpdate = d3.transition(lat)
            .attr("d", function(d) { return line(d.values); });

        lat.exit().remove();

        d3.select(".lat.x.axis").call(xAxis);
        d3.select(".lat.y.axis").call(yAxis);
    };

    latBoxes.on("change", redraw);

    d3.csv(resource, function(data) {

        latencies = latNames.map(function(name) {
            return {
                name: name,
                values: data.map(function(d) {
                    return {elapsed: d.elapsed, value: d[name]};
                })
            };
        });

        redraw();
    });
}
