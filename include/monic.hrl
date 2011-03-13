-define(BUFFER_SIZE, 64*1024).

-record(index, {
    key,
    cookie,
    location,
    size,
    last_modified,
    deleted=false
}).

-record(header, {
    key,
    cookie,    
    size,
    last_modified,
    deleted=false
}).

-record(footer, {
    sha
}).
