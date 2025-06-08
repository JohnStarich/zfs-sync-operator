package wireguard

import (
	"context"
	"log/slog"
	"net"
	"net/netip"
	"testing"

	"github.com/johnstarich/zfs-sync-operator/internal/testlog"
	"github.com/stretchr/testify/require"
	"golang.zx2c4.com/wireguard/tun/netstack"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
)

// StartTest is like [Start] but always listens to a port and runs test cleanup
func StartTest(tb testing.TB, addr netip.Addr, presharedKey, localPrivateKey, peerPublicKey wgtypes.Key) (*netstack.Net, netip.AddrPort) {
	tb.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	tb.Cleanup(cancel)
	conn, err := net.ListenUDP("udp", &net.UDPAddr{})
	require.NoError(tb, err)
	listenAddr := conn.LocalAddr().(*net.UDPAddr).AddrPort()
	require.NoError(tb, conn.Close())
	net, err := Start(ctx, Config{
		ListenPort:      int(listenAddr.Port()),
		LocalAddress:    addr,
		LocalPrivateKey: localPrivateKey[:],
		LogHandler:      testlog.NewLogHandler(tb, slog.LevelInfo),
		PeerPublicKey:   peerPublicKey[:],
		PresharedKey:    presharedKey[:],
	})
	require.NoError(tb, err)
	return net, listenAddr
}
