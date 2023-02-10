test:
	@go test -v -race -coverprofile cov.out -memprofile=mem.out -cpuprofile cpu.out
	@go tool cover -html cov.out
