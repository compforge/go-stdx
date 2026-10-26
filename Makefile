.DEFAULT_GOAL := test

GO ?= go
LINT_WORKERS ?= 4
TEST_WORKERS ?= 4
GO_TEST_PARALLEL ?= 1
TEST_PACKAGES ?= ./...

.PHONY: fix lint test

fix:
	$(GO) fmt ./...

lint:
	@unformatted=$$(find . -type d \( -name .git -o -name .worktrees -o -name vendor \) -prune -o -type f -name '*.go' -exec gofmt -l {} +) || exit $$?; \
	if [ -n "$$unformatted" ]; then \
		printf 'Run make fix to format:\n%s\n' "$$unformatted"; \
		exit 1; \
	fi
	$(GO) vet -p $(LINT_WORKERS) ./...

test:
	$(GO) test -p $(TEST_WORKERS) -parallel $(GO_TEST_PARALLEL) $(TEST_PACKAGES)
