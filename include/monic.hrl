-define(BUFFER_SIZE, 64*1024).

-record(index, {
    key :: binary(),
    cookie :: integer(),
    location :: integer(),
    size :: integer(),
    last_modified,
    deleted=false :: boolean()
}).

-record(header, {
    key :: binary(),
    cookie :: integer(),
    size :: integer(),
    last_modified,
    deleted=false :: boolean()
}).

-record(footer, {
    sha :: binary()
}).
