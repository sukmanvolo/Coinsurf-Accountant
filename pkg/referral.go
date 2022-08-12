package pkg

import "net"

type Referral interface {
	Register(nodeId string, ip net.IP) error
	Payout() error
}
