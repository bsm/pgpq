default: test

test:
	go test ./...

lint:
	golangci-lint run

db.test.create:
	echo "CREATE DATABASE pgpq_test WITH TEMPLATE = template0 ENCODING = 'UTF8'"  | psql -q postgres
db.test.drop:
	echo "DROP DATABASE pgpq_test"  | psql -q postgres

README.md: README.md.tpl $(wildcard *.go)
	becca -package github.com/bsm/pgpq
