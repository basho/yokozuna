function init_throughput() {
    var x = d3.scale.linear().range([0, width]);
    var y = d3.scale.linear().range([height, 0]);
    var xAxis = d3.svg.axis().scale(x).orient("bottom");
    var yAxis = d3.svg.axis().scale(y).orient("left");

    var line = d3.svg.line()
        .x(function(d) { return x(d.elapsed); })
        .y(function(d) { return y(d.successful / d.window); });

    var svg = d3.select("#throughput p.vis").append("svg")
        .attr("id", "throughput")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    d3.csv("summary.csv", function(data) {
        x.domain(d3.extent(data, function(d) { return d.elapsed; }));
        y.domain([0, d3.max(data, function(d) { return (d.total / d.window); })]);

        svg.append("g")
            .attr("class", "x axis")
            .attr("transform", "translate(0," + height + ")")
            .call(xAxis);

        svg.append("g")
            .attr("class", "y axis")
            .call(yAxis);

        svg.append("text")
            .attr("text-anchor", "middle")
            .attr("transform", "translate(" + -(margin.left/2) + "," + (height/2) + ")rotate(-90)")
            .attr("class", "label")
            .text("Ops per s");

        svg.append("path")
            .datum(data)
            .attr("class", "line")
            .attr("d", line);
    });
}
