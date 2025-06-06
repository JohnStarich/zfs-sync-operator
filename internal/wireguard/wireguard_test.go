package wireguard

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"testing"
	"time"

	"github.com/johnstarich/zfs-sync-operator/internal/testlog"
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

	const serverVPNAddr = "192.168.4.29"
	const someMessage = "Hello, World!"
	serverAddr := startHTTPServer(t, netip.MustParseAddr(serverVPNAddr), presharedKey, serverPrivateKey, clientPublicKey, someMessage)
	serverHTTPURL := url.URL{
		Scheme: "http",
		Host:   serverVPNAddr,
	}

	const clientVPNAddr = "192.168.4.28"
	t.Run("happy path", func(t *testing.T) {
		t.Parallel()
		httpClient := makeHTTPClient(t, netip.MustParseAddr(clientVPNAddr), presharedKey, clientPrivateKey, serverPublicKey, serverAddr)
		resp, err := httpClient.Get(serverHTTPURL.String())
		require.NoError(t, err)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, someMessage, string(body))
	})

	t.Run("busted wireguard client key breaks connection", func(t *testing.T) {
		t.Parallel()
		// Then verify an almost identical client with a busted key fails to connect.
		// This ensures we didn't just happen to bind to all interfaces and communicate outside WireGuard.
		bustedPrivateKey := clientPrivateKey[:]
		bustedPrivateKey[2] = 'f'
		bustedPrivateKey[3] = 'o'
		bustedPrivateKey[4] = 'o'
		bustedPrivateWireGuardKey := wgtypes.Key(bustedPrivateKey)
		httpClient := makeHTTPClient(t, netip.MustParseAddr(clientVPNAddr), presharedKey, bustedPrivateWireGuardKey, serverPublicKey, serverAddr)
		_, err := httpClient.Get(serverHTTPURL.String())
		require.EqualError(t, err, `Get "http://192.168.4.29": context deadline exceeded (Client.Timeout exceeded while awaiting headers)`)
	})
}

func startHTTPServer(t *testing.T, addr netip.Addr, presharedKey, privateKey, peerPublicKey wgtypes.Key, message string) netip.AddrPort {
	serverNet, addrPort := StartTestServer(t, addr, presharedKey, privateKey, peerPublicKey)
	listener, err := serverNet.ListenTCP(&net.TCPAddr{Port: 80})
	require.NoError(t, err)

	server := http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Logf("Request received: %s - %s - %s\n", r.RemoteAddr, r.URL.String(), r.UserAgent())
			io.WriteString(w, message)
		}),
	}
	t.Cleanup(func() {
		require.NoError(t, server.Close())
	})
	go server.Serve(listener)
	return addrPort
}

func makeHTTPClient(t *testing.T, addr netip.Addr, presharedKey, privateKey, peerPublicKey wgtypes.Key, peerAddr netip.AddrPort) *http.Client {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	clientNet, err := Connect(ctx, addr, Config{
		LogHandler:    testlog.NewLogHandler(t, slog.LevelDebug),
		PresharedKey:  presharedKey[:],
		PrivateKey:    privateKey[:],
		PeerPublicKey: peerPublicKey[:],
		PeerAddress:   &peerAddr,
	})
	require.NoError(t, err)
	return &http.Client{
		Transport: &http.Transport{
			DialContext: clientNet.DialContext,
		},
		Timeout: 5 * time.Second, // arbitrarily large timeout for a test
	}
}
