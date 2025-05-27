package wireguard

import (
	"context"
	"net"
	"net/http"
	"os"
	"os/signal"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
)

// TODO this currently requires sudo to run
func TestConnect(t *testing.T) {
	t.Parallel()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	t.Cleanup(cancel)
	ctx, cancel = context.WithTimeout(ctx, 5*time.Second)
	t.Cleanup(cancel)

	// presharedKey, err := wgtypes.GenerateKey()
	// require.NoError(t, err)
	privateKeyClient, err := wgtypes.GeneratePrivateKey()
	require.NoError(t, err)
	privateKeyServer, err := wgtypes.GeneratePrivateKey()
	require.NoError(t, err)

	const wireguardPort = 58120
	dialerServer, err := Connect(ctx, Config{
		PrivateKey:    privateKeyServer.String(),
		PeerPublicKey: privateKeyClient.PublicKey().String(),
		ListenPort:    wireguardPort,
		AllowedIPs: []net.IPNet{
			{
				IP:   net.ParseIP("0.0.0.0"),
				Mask: net.CIDRMask(0, 32),
			},
		},
	})
	require.NoError(t, err)
	listenerServer, err := dialerServer.ListenTCP(&net.TCPAddr{Port: 80})
	require.NoError(t, err)
	go func() {
		server := http.Server{
			Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			}),
		}
		_ = server.Serve(listenerServer)
	}()
	time.Sleep(100 * time.Millisecond)

	dialerClient, err := Connect(ctx, Config{
		PrivateKey:    privateKeyClient.String(),
		PeerPublicKey: privateKeyServer.PublicKey().String(),
		PeerIP:        "127.0.0.1",
		PeerPort:      wireguardPort,
		AllowedIPs: []net.IPNet{
			{IP: net.ParseIP("0.0.0.0"), Mask: net.CIDRMask(0, 32)}, // 0.0.0.0/0
		},
	})
	require.NoError(t, err)
	httpClient := http.Client{
		Transport: &http.Transport{
			DialContext: dialerClient.DialContext,
		},
	}
	resp, err := httpClient.Get("http://10.3.0.3")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// addrs, err := dialerClient.LookupHost("google.com")
	// require.NoError(t, err)
	// require.NotEmpty(t, addrs)
	// conn, err := dialerClient.DialTCP(net.TCPAddrFromAddrPort(netip.MustParseAddrPort(net.JoinHostPort(addrs[0], "80"))))
	// require.NoError(t, err)
	// assert.NotNil(t, conn)
}

func startServer(t *testing.T) {
	t.Helper()
}
