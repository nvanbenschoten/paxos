GO     ?= go
DEP    ?= dep
PROTOC ?= protoc

TARGETS := server client
PKGS := $(shell go list ./... | grep -v /vendor)

.PHONY: build
build:
	@for target in $(TARGETS) ; do \
		$(GO) build ./cmd/$$target ; \
	done

.PHONY: clean
clean:
	@$(RM) $(TARGETS)

.PHONY: test
test:
	@$(GO) test -v ./paxos

.PHONY: deps
deps:
	@$(DEP) ensure -update

.PHONY: proto
proto:
	@$(PROTOC) --go_out=plugins=grpc:. -I=.:../../../ paxos/**/*.proto
	@$(PROTOC) --go_out=plugins=grpc:. -I=.:../../../ transport/**/*.proto

.PHONY: check
check:
	@$(GO) vet $(PKGS)
