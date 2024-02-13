package epmd

import (
	"time"
)

const (
	registrarName    string = "EPMD"
	registrarRelease string = "R1"

	defaultRegistrarPort uint16        = 4499
	defaultKeepAlive     time.Duration = 3 * time.Second
)
