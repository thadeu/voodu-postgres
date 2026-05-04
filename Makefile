# voodu-postgres — Makefile
#
# Build targets produce the `voodu-postgres` binary under bin/, which
# is the real plugin command. The shell wrappers (bin/expand and the
# plugin commands added in later milestones) exec it with the right
# subcommand — that's how the Voodu plugin loader discovers each
# command by name.

BIN      := bin/voodu-postgres
PKG      := ./cmd/voodu-postgres
DIST     := dist
VERSION  := $(shell grep '^version:' plugin.yml | awk '{print $$2}')

GO       := go
LDFLAGS  := -s -w -X main.version=$(VERSION)

.PHONY: build test lint cross clean install-local

build:
	$(GO) build -ldflags '$(LDFLAGS)' -o $(BIN) $(PKG)

test:
	$(GO) test ./...

lint:
	$(GO) vet ./...

# cross produces the two release binaries we ship. CGO disabled so the
# binary runs on any glibc/musl Linux without plugin-side surprises.
cross: $(DIST)/voodu-postgres_linux_amd64 $(DIST)/voodu-postgres_linux_arm64

$(DIST)/voodu-postgres_linux_amd64:
	@mkdir -p $(DIST)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -ldflags '$(LDFLAGS)' -o $@ $(PKG)

$(DIST)/voodu-postgres_linux_arm64:
	@mkdir -p $(DIST)
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 $(GO) build -ldflags '$(LDFLAGS)' -o $@ $(PKG)

# install-local drops the current build into a plugins root that
# mirrors the server layout. Useful for testing against a local Voodu
# controller without going through `voodu plugins:install`.
install-local: build
	@if [ -z "$(PLUGINS_ROOT)" ]; then \
		echo "PLUGINS_ROOT is required (e.g. /opt/voodu/plugins)"; exit 1; \
	fi
	@mkdir -p $(PLUGINS_ROOT)/postgres/bin
	cp $(BIN) $(PLUGINS_ROOT)/postgres/bin/voodu-postgres
	cp bin/expand $(PLUGINS_ROOT)/postgres/bin/
	chmod +x $(PLUGINS_ROOT)/postgres/bin/*
	cp plugin.yml $(PLUGINS_ROOT)/postgres/
	cp install uninstall $(PLUGINS_ROOT)/postgres/ 2>/dev/null || true
	@echo "installed into $(PLUGINS_ROOT)/postgres"

clean:
	rm -rf $(BIN) $(DIST)
