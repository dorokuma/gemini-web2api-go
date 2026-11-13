export GOTOOLCHAIN := go1.27.1

.PHONY: check
check:
	@files="$$(gofmt -l .)" || exit $$?; \
	if [ -n "$$files" ]; then echo "gofmt failed:"; echo "$$files"; exit 1; fi
	go vet ./...
	go test ./... -count=1
	go build -o /dev/null .
