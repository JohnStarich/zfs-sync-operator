package wireguard

import (
	"context"
	"net"
	"os"
	"os/signal"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TODO this currently requires sudo to run
func TestConnect(t *testing.T) {
	t.Parallel()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	t.Cleanup(cancel)
	ctx, cancel = context.WithTimeout(ctx, 5*time.Second)
	t.Cleanup(cancel)
	config := Config{
		PrivateKey:    "SPOpI5EclKPDJtkI2KCJixa470a/HmFe64iKwBYFRno=",
		PeerPublicKey: "BlIpNobmpiwrzEMi2qZQ8CidyphxRGP/rm9cgUI/snY=",
		PeerIP:        "127.0.0.1",
		PeerPort:      12345,
		AllowedIPs: []net.IPNet{
			{
				IP:   net.ParseIP("1.1.1.1"),
				Mask: net.CIDRMask(32, 32),
			},
		},
	}
	assert.NoError(t, Connect(ctx, config))
}
