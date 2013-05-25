#!/bin/bash
#
# ./plot-3-dimensions.sh <title> <y-label> <dat file>

TITLE=$1; shift
YLABEL=$1; shift
DAT=$1; shift
out=${DAT%.dat}

gnuplot <<EOF

reset
set title "$TITLE"

set terminal svg font "monospace"
set output '$out.svg'

set xtics rotate by -45 offset character -1, -0.5
set ylabel "$YLABEL"

set border linewidth 2
set style line 1 lc rgb '#88BB44' lt 1 lw 2 pt 7 pi -1 ps 1.5
set style line 2 lc rgb '#FF5544' lt 1 lw 2 pt 7 pi -1 ps 1.5
set pointintervalbox 2

set key below autotitle columnhead width 2

set style data linespoints
plot '$DAT' using 2:xtic(1) ls 1, '' u 3 ls 2
EOF
