# Format header
NR == 1 {
    # First remove leading space
    sub(/ +/, "")
    # Next convert space to commas
    gsub(/ +/, ",")
    # Convert 'Time' header to 'timestamp'
    sub(/Time/, "timestamp")
    print
    next
}

# Ignore first record
NR == 2 { next };

# Ignore header reprints
/.*Time.*Int.*rKb.*Sat.*/ { next }

{ gsub(/ +/,","); print }
