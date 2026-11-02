# Go commerce example

The fraud client in `internal/fraud` is generated from the shared
`protocol/commerce/fraud_detection.proto`. To reproduce the checked-in files,
use `protoc` 36.2 and run from the repository root:

```sh
GOBIN="$(mktemp -d)"
export GOBIN
export PATH="$GOBIN:$PATH"
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
protoc -I protocol/commerce \
  --go_out=go/ecomm/internal/fraud --go_opt=paths=source_relative \
  --go_opt=Mfraud_detection.proto=github.com/stove-io/examples/go/ecomm/internal/fraud \
  --go-grpc_out=go/ecomm/internal/fraud --go-grpc_opt=paths=source_relative \
  --go-grpc_opt=Mfraud_detection.proto=github.com/stove-io/examples/go/ecomm/internal/fraud \
  protocol/commerce/fraud_detection.proto
rm -r "$GOBIN"
```

The generated files are checked in so a normal build does not require these tools.
