package wireguard

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
)

func TestConnectToHTTPServer(t *testing.T) {
	t.Parallel()
	presharedKey, err := wgtypes.GenerateKey()
	require.NoError(t, err)

	serverPrivateKey, err := wgtypes.GeneratePrivateKey()
	require.NoError(t, err)
	serverPublicKey := serverPrivateKey.PublicKey()

	clientPrivateKey, err := wgtypes.GeneratePrivateKey()
	require.NoError(t, err)
	clientPublicKey := clientPrivateKey.PublicKey()

	const (
		serverVPNAddr  = "192.168.4.29"
		serverRealAddr = "127.0.0.1"
		serverVPNPort  = 58120
	)
	const someMessage = "Hello, World!"
	go startServer(t, netip.AddrPortFrom(netip.MustParseAddr(serverVPNAddr), serverVPNPort), presharedKey, serverPrivateKey, clientPublicKey, someMessage)
	serverHTTPURL := url.URL{
		Scheme: "http",
		Host:   serverVPNAddr,
	}

	const clientVPNAddr = "192.168.4.28"
	httpClient := makeHTTPClient(t, netip.MustParseAddr(clientVPNAddr), presharedKey, clientPrivateKey, serverPublicKey, netip.AddrPortFrom(netip.MustParseAddr(serverRealAddr), serverVPNPort))
	resp, err := httpClient.Get(serverHTTPURL.String())
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, someMessage, string(body))
}

func startServer(t *testing.T, addr netip.AddrPort, presharedKey, privateKey, peerPublicKey wgtypes.Key, message string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	dialer, err := Connect(ctx, addr.Addr(), Config{
		PresharedKey:  toPointer(presharedKey[:]),
		PrivateKey:    privateKey[:],
		PeerPublicKey: peerPublicKey[:],
		ListenPort:    int(addr.Port()),
	})
	require.NoError(t, err)
	listener, err := dialer.ListenTCP(&net.TCPAddr{Port: 80})
	require.NoError(t, err)

	server := http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Logf("Request received: %s - %s - %s\n", r.RemoteAddr, r.URL.String(), r.UserAgent())
			io.WriteString(w, message)
		}),
	}
	require.NoError(t, server.Serve(listener))
	t.Cleanup(func() {
		require.NoError(t, server.Close())
	})
}

func makeHTTPClient(t *testing.T, addr netip.Addr, presharedKey, privateKey, peerPublicKey wgtypes.Key, peerAddr netip.AddrPort) *http.Client {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	dialer, err := Connect(ctx, addr, Config{
		PresharedKey:  toPointer(presharedKey[:]),
		PrivateKey:    privateKey[:],
		PeerPublicKey: peerPublicKey[:],
		PeerAddr:      &peerAddr,
	})
	require.NoError(t, err)
	return &http.Client{
		Transport: &http.Transport{
			DialContext: dialer.DialContext,
		},
	}
}
