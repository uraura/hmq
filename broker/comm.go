package broker

import (
	"time"

	uuid "github.com/google/uuid"
)

const (
	// ACCEPT_MIN_SLEEP is the minimum acceptable sleep times on temporary errors.
	ACCEPT_MIN_SLEEP = 100 * time.Millisecond
	// ACCEPT_MAX_SLEEP is the maximum acceptable sleep times on temporary errors
	ACCEPT_MAX_SLEEP = 10 * time.Second
)

const (
	CONNECT = uint8(iota + 1)
	CONNACK
	PUBLISH
	PUBACK
	PUBREC
	PUBREL
	PUBCOMP
	SUBSCRIBE
	SUBACK
	UNSUBSCRIBE
	UNSUBACK
	PINGREQ
	PINGRESP
	DISCONNECT
)
const (
	QosAtMostOnce byte = iota
	QosFailure         = 0x80
)

func GenUniqueId() string {
	id, err := uuid.NewRandom()
	if err != nil {
		log.Error("uuid.NewRandom() returned an error: " + err.Error())
	}
	return id.String()
}
