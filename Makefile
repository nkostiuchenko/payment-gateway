MIGRATE_VERSION := v4.18.3
MIGRATE_BIN := $(CURDIR)/.bin/migrate

.PHONY: migrate-pg-up migrate-pg-down migrate-mongo-up migrate-mongo-down

migrate-install:
	mkdir -p .bin
	GOBIN=$(CURDIR)/.bin go install \
		-tags 'postgres mongodb' \
		github.com/golang-migrate/migrate/v4/cmd/migrate@$(MIGRATE_VERSION)

migrate-pg-up:
	$(MIGRATE_BIN) \
		-path ./migrations/postgres \
		-database "$(DATABASE_URL)" \
		up

migrate-pg-down:
	$(MIGRATE_BIN) \
		-path ./migrations/postgres \
		-database "$(DATABASE_URL)" \
		down

migrate-mongo-up:
	$(MIGRATE_BIN) \
		-path ./migrations/mongo \
		-database "$(DATABASE_URL)" \
		up

migrate-mongo-down:
	$(MIGRATE_BIN) \
		-path ./migrations/mongo \
		-database "$(DATABASE_URL)" \
		down
