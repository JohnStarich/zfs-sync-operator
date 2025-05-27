package wireguard

import (
	"context"
	"fmt"
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
	go startServer(t, netip.AddrPortFrom(netip.MustParseAddr(serverVPNAddr), serverVPNPort), serverPrivateKey, clientPublicKey)
	serverHTTPURL := url.URL{
		Scheme: "http",
		Host:   serverVPNAddr,
	}

	const clientVPNAddr = "192.168.4.28"
	httpClient := makeHTTPClient(t, netip.MustParseAddr(clientVPNAddr), clientPrivateKey, netip.AddrPortFrom(netip.MustParseAddr(serverRealAddr), serverVPNPort), serverPublicKey)
	resp, err := httpClient.Get(serverHTTPURL.String())
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, "Hello from userspace TCP!", string(body))
}

func startServer(t *testing.T, addr netip.AddrPort, privateKey wgtypes.Key, peerPublicKey wgtypes.Key) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	dialer, err := Connect(ctx, addr.Addr(), Config{
		PrivateKey:    privateKey[:],
		PeerPublicKey: peerPublicKey[:],
		ListenPort:    int(addr.Port()),
	})
	require.NoError(t, err)
	listener, err := dialer.ListenTCP(&net.TCPAddr{Port: 80})
	require.NoError(t, err)

	server := http.Server{
		Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			fmt.Printf("> %s - %s - %s\n", request.RemoteAddr, request.URL.String(), request.UserAgent())
			io.WriteString(writer, "Hello from userspace TCP!")
		}),
	}
	require.NoError(t, server.Serve(listener))
	t.Cleanup(func() {
		require.NoError(t, server.Close())
	})
}

func makeHTTPClient(t *testing.T, addr netip.Addr, privateKey wgtypes.Key, peerAddr netip.AddrPort, peerPublicKey wgtypes.Key) *http.Client {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	dialer, err := Connect(ctx, addr, Config{
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
