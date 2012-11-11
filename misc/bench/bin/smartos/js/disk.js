function init_disks(disks) {
    // stores the transformed data for each disk
    var diskData = [];

    var x = d3.time.scale().range([0, width]);
    var y = d3.scale.linear().range([height, 0]);
    var xAxis = d3.svg.axis().scale(x).orient("bottom");
    var yAxis = d3.svg.axis().scale(y).orient("left");
    var colors = d3.scale.category20();

    var line = d3.svg.line()
        .x(function(d) { return x(d.timestamp); })
        .y(function(d) { return y(d["%b"]); });

    var svg = d3.select("#disk p.vis").append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    var parseDate = d3.time.format("%Y-%m-%dT%H:%M:%S").parse;

    svg.append("g")
        .attr("class", "db x axis")
        .attr("transform", "translate(0," + height + ")")
        .call(xAxis);

    svg.append("g")
        .attr("class", "db y axis")
        .call(yAxis);

    svg.append("text")
        .attr("text-anchor", "middle")
        .attr("transform", "translate(" + -(margin.left/2) + "," + (height/2) + ")rotate(-90)")
        .attr("class", "label")
        .text("Disk %b");

    var redraw = function() {

        var names = diskData.map(function(d) { return d.name; });
        colors.domain(names);

        // length of time is same for all, just pull from first
        x.domain([
            d3.min(diskData, function(c) {
                return d3.min(c.values, function(d) { return d.timestamp; })
            }),
            d3.max(diskData, function(c) {
                return d3.max(c.values, function(d) { return d.timestamp; })
            })
        ]);
        y.domain([0,100]);


        var busy = svg.selectAll(".disk_busy")
            .data(diskData, function(d) { return d.name; });

        busy.enter().append("path")
            .attr("class", "line disk_busy")
            .attr("d", function(d) { return line(d.values); })
            .style("stroke", function(d) { return colors(d.name); });

        d3.transition(busy)
            .attr("d", function(d) { return line(d.values); });

        busy.exit().remove();

        d3.select(".db.x.axis").call(xAxis);
        d3.select(".db.y.axis").call(yAxis);
    };

    var add_disk_data = function(name, resource) {
        d3.csv(resource, function(data) {
            data.forEach(function(d) {
                d.timestamp = parseDate(d.timestamp)
            });

            diskData.push({name:name, values:data});
            redraw();
        })
    };

    disks.forEach(function(d) { add_disk_data(d.name, d.resource); });
};
