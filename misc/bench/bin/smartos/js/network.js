// ydomain should be "absolute" or "relative"
function init_nics(nics, ycol, cclass, label, ydomain) {
    // stores the transformed data for each nic
    var nicData = [];

    var x = d3.time.scale().range([0, width]);
    var y = d3.scale.linear().range([height, 0]);
    var xAxis = d3.svg.axis().scale(x).orient("bottom");
    var yAxis = d3.svg.axis().scale(y).orient("left");
    var colors = d3.scale.category20();

    var line = d3.svg.line()
        .x(function(d) { return x(d.timestamp); })
        .y(function(d) { return y(d[ycol]); });

    var svg = d3.select("#network p.vis").append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    var parseDate = d3.time.format("%H:%M:%S").parse;

    svg.append("g")
        .attr("class", cclass + " x axis")
        .attr("transform", "translate(0," + height + ")")
        .call(xAxis);

    svg.append("g")
        .attr("class", cclass + " y axis")
        .call(yAxis);

    svg.append("text")
        .attr("text-anchor", "middle")
        .attr("transform", "translate(" + -(margin.left/2) + "," + (height/2) + ")rotate(-90)")
        .attr("class", "label")
        .text(label);

    var redraw = function() {

        var names = nicData.map(function(d) { return d.name; });
        colors.domain(names);

        x.domain([
            d3.min(nicData, function(c) {
                return d3.min(c.values, function(d) { return d.timestamp; })
            }),
            d3.max(nicData, function(c) {
                return d3.max(c.values, function(d) { return d.timestamp; })
            })
        ]);
        if (ydomain === "absolute") {
            y.domain([0,100]);
        } else {
            y.domain([
                d3.min(nicData, function(c) {
                    return d3.min(c.values, function(d) { return d[ycol]; })
                }),
                d3.max(nicData, function(c) {
                    return d3.max(c.values, function(d) { return d[ycol]; })
                })
            ]);
        }

        var lines = svg.selectAll("." + cclass + "_line")
            .data(nicData, function(d) { return d.name; });

        lines.enter().append("path")
            .attr("class", "line " + cclass + "_line")
            .attr("d", function(d) { return line(d.values); })
            .style("stroke", function(d) { return colors(d.name); });

        d3.transition(lines)
            .attr("d", function(d) { return line(d.values); });

        lines.exit().remove();

        d3.select("." + cclass + ".x.axis").call(xAxis);
        d3.select("." + cclass + ".y.axis").call(yAxis);
    };

    var add_nic_data = function(name, resource) {
        d3.csv(resource, function(data) {
            var values = data.map(function(d) {
                var tmp = {timestamp: parseDate(d.timestamp)};
                tmp[ycol] = +d[ycol];
                return tmp;
            });

            nicData.push({name:name, values:values});
            redraw();
        })
    };

    nics.forEach(function(d) { add_nic_data(d.name, d.resource); });
};
