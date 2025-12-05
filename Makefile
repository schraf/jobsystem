all: vet test

vet:
	@echo "Vetting code..."
	@go vet ./...

fmt:
	@echo "Formatting code..."
	@go fmt ./...

test:
	@echo "Running tests..."
	@go test ./...

deps:
	@echo "Installing dependencies..."
	@go mod download
	@go mod tidy

help:
	@echo "Available targets:"
	@echo "  all    - Run vet and test"
	@echo "  vet    - Vet the code"
	@echo "  fmt    - Format the code"
	@echo "  test   - Run tests"
	@echo "  deps   - Install dependencies"
	@echo ""
	@echo "  help   - Show this help message"
