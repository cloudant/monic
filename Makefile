ERL ?= erl
APP := monic

.PHONY: deps

all: deps
	@(./rebar compile)

deps:
	@(./rebar get-deps)

clean:
	@(./rebar clean)

distclean: clean
	@(./rebar delete-deps)

rel: all
	@(./rebar generate)

docs:
	@erl -noshell -run edoc_run application '$(APP)' '"."' '[]'

test: all
	@(./rebar skip_deps=true eunit)
