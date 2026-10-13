-- +goose Up
-- +goose StatementBegin
CREATE TABLE payments (
    id 			UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    amount 		BIGINT NOT NULL CHECK (amount >= 0),
    provider 	TEXT NOT NULL,
    status 		TEXT NOT NULL,
    metadata 	JSONB,
    created_at 	TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at 	TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE payment_events (
    id 			UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    payment_id 	UUID NOT NULL REFERENCES payments(id),
    payload 	JSONB NOT NULL,
    created_at 	TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE payments;
DROP TABLE payment_events;
-- +goose StatementEnd
