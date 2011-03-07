-define(INDEX_SIZE, 28).
-define(HEADER_SIZE, 25).
-define(FOOTER_SIZE, 24).

-record(index, {
    key,
    location,
    size,
    version,
    flags
}).

-record(header, {
    key,
    cookie,
    size,
    version,
    flags
}).

-record(footer, {
    sha
}).
