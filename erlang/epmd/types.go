package epmd

import (
	"time"
)

const (
	registrarName    string = "EPMD"
	registrarRelease string = "R1"

	defaultKeepAlive time.Duration = 3 * time.Second

	defaultEPMDPort uint16 = 4369

	epmdAliveReq      = 120
	epmdAliveResp     = 121
	epmdAliveRespX    = 118
	epmdPortPleaseReq = 122
	epmdPortResp      = 119
	epmdNamesReq      = 110
)
