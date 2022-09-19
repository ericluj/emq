consumer:
	export GO111MODULE=on && go run apps/consumer/main.go

producer:
	export GO111MODULE=on && go run apps/producer/main.go

lookupd:
	export GO111MODULE=on && go run apps/emqlookupd/main.go

emq:
	export GO111MODULE=on && go run apps/emqd/main.go

lint:
	golangci-lint run ./...

image:
	docker build -t emqd:v1 -f deploy/emqd/Dockerfile .
	docker build -t emqlookupd:v1 -f deploy/emqlookupd/Dockerfile .

up:
	docker-compose up -d

stop:
	docker-compose stop

rm:
	docker-compose rm

recreate:
	docker-compose up -d --force-recreate