test_postgres: start_postgres
	go test -timeout=20s -race -v $$(glide novendor)

start_postgres:
	docker-compose up -d --build fluent-postgres-db
	sleep 10

stop_postgres:
	docker-compose down
