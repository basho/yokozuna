# Need to skip the first N lines which contain stats since boot.  Look
# for 1st occurance of date string after the first line.  Ignore
# everything before that.
BEGIN {
    # ignore first line with this getline call
    getline

    ts_regex = "^[[:digit:]]+$"
    while ((getline tmp) > 0) {
        if (tmp ~ /^device.*/) {
            # remove the trailing comma
            sub(/%b,/, "%b", tmp)
            printf "timestamp,%s\n", tmp
        } else if (tmp ~ ts_regex) {
            ts=tmp
            break
        }
    }
}

/^extended.*/ { next }

/^device.*/ { next }

$0 ~ ts_regex {
    ts=$0
    next
}

{ printf "%s,%s\n", strftime("%FT%T", ts), $0 }

