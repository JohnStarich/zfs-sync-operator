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

func StartTestServer(tb testing.TB, addr netip.Addr, presharedKey, privateKey, peerPublicKey wgtypes.Key) (*netstack.Net, netip.AddrPort) {
	tb.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	tb.Cleanup(cancel)
	conn, err := net.ListenUDP("udp", &net.UDPAddr{})
	require.NoError(tb, err)
	listenAddr := conn.LocalAddr().(*net.UDPAddr).AddrPort()
	require.NoError(tb, conn.Close())
	serverNet, err := Connect(ctx, Config{
		LocalAddress:  addr,
		LogHandler:    testlog.NewLogHandler(tb, slog.LevelInfo),
		PresharedKey:  presharedKey[:],
		PrivateKey:    privateKey[:],
		PeerPublicKey: peerPublicKey[:],
		ListenPort:    int(listenAddr.Port()),
	})
	require.NoError(tb, err)
	return serverNet, listenAddr
}
