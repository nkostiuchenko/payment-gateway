package common

const (
	CommandsTopic = "payment.commands"
	EventsTopic   = "payment.events"
	DLQTopic      = "payment.dlq"
)

const (
	StatusProcessing = "PROCESSING"
	StatusCompleted  = "COMPLETED"
	StatusFailed     = "FAILED"
)

const (
	CommandCreatePayment = "CreatePayment"
)

const (
	EventPaymentCreated      = "PaymentCreated"
	EventProcessingStarted   = "ProcessingStarted"
	EventProcessingFailed    = "ProcessingFailed"
	EventProcessingSucceeded = "ProcessingSucceeded"
)

const (
	CommandsConsumerGroup = "commands-consumer-group"
	EventsConsumerGroup   = "events-consumer-group"
)

const (
	WebhookStatusSuccess = "success"
	WebhookStatusError   = "error"
)

const (
	MongoPaymentsCollection = "payments"
)
