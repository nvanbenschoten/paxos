GO     ?= go
DEP    ?= dep
PROTOC ?= protoc

TARGET := paxos

.PHONY: build
build:
	@$(GO) build

.PHONY: clean
clean:
	@$(RM) $(TARGET)

.PHONY: deps
deps:
	@$(DEP) ensure -update

.PHONY: proto
proto:
	@$(PROTOC) --go_out=plugins=grpc:. **/*.proto
