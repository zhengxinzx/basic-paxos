# Makefile

# Default target
all: test

# Test target
test:
	@echo "Running tests..."
	@go test ./... -v

.PHONY: test
