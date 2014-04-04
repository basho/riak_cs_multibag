.PHONY: deps test depgraph graphviz all compile

all: compile

compile: deps
	@(./rebar compile)

deps:
	@./rebar get-deps

clean:
	@./rebar clean

# TODO: not examined. copied from riak_cs
DIALYZER_APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto inets \
	xmerl webtool eunit syntax_tools compiler
PLT ?= $(HOME)/.riak-cs-bag_dialyzer_plt

include tools.mk
