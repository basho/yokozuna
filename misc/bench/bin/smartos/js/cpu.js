function init_cpus(cpus) {
    var cpuData = [];
    var yCol = "CPU";
    var x = d3.time.scale().range([0, width]);
    var y = d3.scale.linear().range([height, 0]);
    var xAxis = d3.svg.axis().scale(x).orient("bottom");
    var yAxis = d3.svg.axis().scale(y).orient("left");
    var colors = d3.scale.category20();
    var parseDate = d3.time.format("%Y-%m-%dT%H:%M:%S").parse;

    var line = d3.svg.line()
        .x(function(d) { return x(d.timestamp); })
        .y(function(d) { return y(d[yCol]); });

    var svg = d3.select("#cpu p.vis").append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    svg.append("g")
        .attr("class", "cpuu x axis")
        .attr("transform", "translate(0," + height + ")")
        .call(xAxis);

    svg.append("g")
        .attr("class", "cpuu y axis")
        .call(yAxis);

    svg.append("text")
        .attr("text-anchor", "middle")
        .attr("transform", "translate(" + -(margin.left/2) + "," + (height/2) + ")rotate(-90)")
        .attr("class", "label")
        .text("CPU %");

    var redraw = function() {
        var keys = cpuData.map(function(d) { return d.key; });
        colors.domain(keys);

        x.domain([
            d3.min(cpuData, function(c) {
                return d3.min(c.values, function(d) { return d.timestamp; })
            }),
            d3.max(cpuData, function(c) {
                return d3.max(c.values, function(d) { return d.timestamp; })
            })
        ]);
        y.domain([0,100]);

        var usage = svg.selectAll(".cpu_usage")
            .data(cpuData, function(d) { return d.key; });

        usage.enter().append("path")
            .attr("class", "line")
            .attr("d", function(d) { return line(d.values); })
            .style("stroke", function(d) { return colors(d.key); });

        d3.transition(usage)
            .attr("d", function(d) { return line(d.values); });

        usage.exit().remove();

        d3.select(".cpuu.x.axis").call(xAxis);
        d3.select(".cpuu.y.axis").call(yAxis);

    };

    var add_cpu_data = function(name, resource) {
        d3.csv(resource, function(data) {
            // scrub data to essential elements
            data = data.map(function(d) {
                var tmp = {process: d["PROCESS/NLWP"],
                           timestamp: parseDate(d.timestamp)};
                tmp[yCol] = +d[yCol];
                return tmp;
            });

            data = d3.nest()
                .key(function(d) { return name + "-" + d.process; })
                .entries(data);

            cpuData = d3.merge([cpuData, data]);
            redraw();
        })
    };

    cpus.forEach(function(d) { add_cpu_data(d.name, d.resource); });
};
