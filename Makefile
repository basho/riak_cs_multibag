.PHONY: deps test depgraph graphviz all compile

all: compile

compile: deps
	@(./rebar compile)

deps:
	@./rebar get-deps

clean:
	@./rebar clean

DIALYZER_APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto \
	inets eunit
PLT ?= $(HOME)/.riak-cs-multibag_dialyzer_plt

include tools.mk
