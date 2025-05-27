package wireguard

import (
	"context"
	"io"
	"log/slog"
	"net/netip"
	"os"
	"os/signal"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
)

func TestConnect(t *testing.T) {
	t.Parallel()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	t.Cleanup(cancel)
	ctx, cancel = context.WithTimeout(ctx, 5*time.Second)
	t.Cleanup(cancel)

	serverPrivateKey, err := wgtypes.GeneratePrivateKey()
	require.NoError(t, err)
	serverPublicKey := serverPrivateKey.PublicKey()
	clientPrivateKey, err := wgtypes.GeneratePrivateKey()
	require.NoError(t, err)
	clientPublicKey := clientPrivateKey.PublicKey()

	go func() {
		_, err := StartServer(ctx, slog.Default(), netip.MustParseAddrPort("192.168.4.29:58120"), serverPrivateKey, clientPublicKey)
		require.NoError(t, err)
	}()
	httpClient, err := StartClient(ctx, slog.Default(), netip.MustParseAddr("192.168.4.28"), clientPrivateKey, netip.MustParseAddrPort("127.0.0.1:58120"), serverPublicKey)
	require.NoError(t, err)

	resp, err := httpClient.Get("http://192.168.4.29/")
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, "Hello from userspace TCP!", string(body))
}
