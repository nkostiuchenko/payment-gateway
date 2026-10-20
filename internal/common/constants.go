package common

const (
	CommandsTopic = "payment.commands"
	EventsTopic   = "payment.events"
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
