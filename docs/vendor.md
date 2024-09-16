# vendor


## overview 

`mariv2` utilizes go vendor, which creates a directory in the project with source code for all external dependencies. The vendor directory helps serve two purposes:

1. lock the project to specific dependency versions
2. avoid external api calls to fetch source code

This keeps `mariv2` both secure with tested dependencies and aligns to the project being an embedded database.


## usage

### create vendor

to create the vendor directory (just as reference), run the following:
```bash
cd ./mariv2
go mod tidy
go mod vendor
```

vendor is bundled with the `mariv2` source code so the above is not required.

### build with vendor

To build using vendor, use:
```bash
go build -mod=vendor
```

the `-mod` flag can be avoided if `env` is set with:
```bash
export GOFLAGS="-mod=vendor"
```