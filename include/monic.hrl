-define(BUFFER_SIZE, 64*1024).
-define(INDEX_SIZE, 32).
-define(HEADER_SIZE, 28).
-define(FOOTER_SIZE, 24).

-record(index, {
    key,
    cookie,
    location,
    size,
    version, %% last_modified,
    flags
}).

-record(header, {
    key,
    cookie,    
    size,
    version, %% last_modified,
    flags
}).

-record(footer, {
    sha
}).
