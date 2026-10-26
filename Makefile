.DEFAULT_GOAL := test

GO ?= go
GOFMT ?= gofmt
LINT_WORKERS ?= 4
TEST_WORKERS ?= 4
GO_TEST_PARALLEL ?= 1
TEST_PACKAGES ?= ./...
TEST_FILES ?=
LINT_FILES ?=

# File selections are space-separated paths relative to this module.
# Analyze whole packages; unknown/deleted or non-Go inputs require a full check.
existing_go_files = $(wildcard $(filter %.go,$(1)))
packages_for = $(if $(strip $(1)),$(if $(filter-out $(call existing_go_files,$(1)),$(1)),./...,$(sort $(addprefix ./,$(dir $(patsubst ./%,%,$(1)))))),./...)
LINT_PACKAGES = $(call packages_for,$(LINT_FILES))
SELECTED_TEST_PACKAGES = $(if $(strip $(TEST_FILES)),$(call packages_for,$(TEST_FILES)),$(TEST_PACKAGES))

.PHONY: fix lint test

fix:
ifneq ($(strip $(LINT_FILES)),)
	$(if $(call existing_go_files,$(LINT_FILES)),$(GOFMT) -w $(call existing_go_files,$(LINT_FILES)),@echo "No existing Go files selected for formatting.")
else
	$(GO) fmt ./...
endif

lint:
ifneq ($(strip $(LINT_FILES)),)
	@unformatted=$$($(if $(call existing_go_files,$(LINT_FILES)),$(GOFMT) -l $(call existing_go_files,$(LINT_FILES)),true)) || exit $$?; \
	if [ -n "$$unformatted" ]; then \
		printf 'Run make fix with the same LINT_FILES to format:\n%s\n' "$$unformatted"; \
		exit 1; \
	fi
else
	@unformatted=$$(find . -type d \( -name .git -o -name .worktrees -o -name vendor \) -prune -o -type f -name '*.go' -exec $(GOFMT) -l {} +) || exit $$?; \
	if [ -n "$$unformatted" ]; then \
		printf 'Run make fix to format:\n%s\n' "$$unformatted"; \
		exit 1; \
	fi
endif
	$(GO) vet -p $(LINT_WORKERS) $(LINT_PACKAGES)

test:
	$(GO) test -p $(TEST_WORKERS) -parallel $(GO_TEST_PARALLEL) $(SELECTED_TEST_PACKAGES)
