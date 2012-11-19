# Need to skip the first N lines which contain stats since boot.  Look
# for 1st occurance of timestamp after the first line.  Ignore
# everything before that.
BEGIN {
    # ignore first line with this getline call
    getline

    ts_regex = "^[[:digit:]]+$"
    while ((getline tmp) > 0) {
        if (tmp ~ /.*r b w.*/) {
            # First remove leading spaces from header
            sub(/ +/, "", tmp)
            # Next convert spaces to commas
            gsub(/ +/, ",", tmp)
            printf "timestamp,%s\n", tmp
        } else if (tmp ~ ts_regex) {
            ts=tmp
            break
        }
    }
}

/kthr.*memory.*/ { next }

/.*r b w.*/ { next }

$0 ~ ts_regex {
    ts=$0
    next
}

{
    printf "%s", strftime("%FT%T", ts)
    gsub(/ +/,",")
    print
}

