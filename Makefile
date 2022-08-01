consumer:
	go run apps/consumer/main.go

producer:
	go run apps/producer/main.go

emqd:
	go run apps/emqd/main.go

lookupd:
	go run apps/emqlookupd/main.go