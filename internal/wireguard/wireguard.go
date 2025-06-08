// Package wireguard starts WireGuard peers natively in Go.
// Does not require any additional Linux capabilities.
package wireguard

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/netip"

	"github.com/johnstarich/zfs-sync-operator/internal/pointer"
	"golang.zx2c4.com/wireguard/tun/netstack"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
)

// Config contains data to configure a WireGuard connection
type Config struct {
	DNSAddresses    []netip.Addr // Defaults to CloudFlare DNS (1.0.0.1, 1.1.1.1).
	ListenPort      int          // Optional: The port number to listen for WireGuard connections.
	LocalAddress    netip.Addr
	LocalPrivateKey []byte
	LogHandler      slog.Handler // Defaults to slog.Default().
	PeerAddress     *string      // Optional: The address to connect to a remote WireGuard peer.
	PeerPublicKey   []byte
	PresharedKey    []byte // Optional, but highly recommended.
}

// Start starts a new WireGuard interface and configures it with details from a [Config].
// This can be used to start a peer listening on a port, or only connect to a peer address.
func Start(ctx context.Context, config Config) (*netstack.Net, error) {
	var listenPort *int
	if config.ListenPort != 0 {
		listenPort = pointer.Of(config.ListenPort)
	}
	var presharedKey *wgtypes.Key
	if len(config.PresharedKey) > 0 {
		presharedKey = pointer.Of(wgtypes.Key(config.PresharedKey))
	}
	var address *net.UDPAddr
	if config.PeerAddress != nil {
		udpAddr, err := net.ResolveUDPAddr("udp", *config.PeerAddress)
		if err != nil {
			return nil, err
		}
		address = udpAddr
	}
	wgConfig := wgtypes.Config{
		PrivateKey:   pointer.Of(wgtypes.Key(config.LocalPrivateKey)),
		ListenPort:   listenPort,
		ReplacePeers: true,
		Peers: []wgtypes.PeerConfig{
			{
				PublicKey:         wgtypes.Key(config.PeerPublicKey),
				PresharedKey:      presharedKey,
				Endpoint:          address,
				ReplaceAllowedIPs: true,
				AllowedIPs: []net.IPNet{
					{IP: net.ParseIP("0.0.0.0"), Mask: net.CIDRMask(0, 32)},
				},
			},
		},
	}

	logger := slog.Default()
	if config.LogHandler != nil {
		logger = slog.New(config.LogHandler)
	}
	iface := newInterface(logger, config.LocalAddress, config.DNSAddresses)
	device, dialer, err := iface.Start(ctx)
	if err != nil {
		return nil, err
	}
	go func() {
		err := iface.Wait()
		if err != nil {
			logger.Error("Failed on wait for interface", slog.Any("error", err))
		}
	}()

	var configBuffer bytes.Buffer
	writeConfig(&configBuffer, wgConfig)
	if err := device.IpcSetOperation(&configBuffer); err != nil {
		return nil, err
	}
	if err := device.Up(); err != nil {
		return nil, err
	}
	return dialer, nil
}

func writeConfig(w io.Writer, cfg wgtypes.Config) {
	writeIf(w, cfg.PrivateKey != nil, "private_key=%s\n", keyToHex(valueOrZeroValue(cfg.PrivateKey)))
	writeIf(w, cfg.ListenPort != nil, "listen_port=%d\n", valueOrZeroValue(cfg.ListenPort))
	writeIf(w, cfg.FirewallMark != nil, "fwmark=%d\n", valueOrZeroValue(cfg.FirewallMark))
	writeIf(w, cfg.ReplacePeers, "replace_peers=true\n")
	for _, p := range cfg.Peers {
		mustWritef(w, "public_key=%s\n", keyToHex(p.PublicKey))
		writeIf(w, p.Remove, "remove=true\n")
		writeIf(w, p.UpdateOnly, "update_only=true\n")
		writeIf(w, p.PresharedKey != nil, "preshared_key=%s\n", keyToHex(valueOrZeroValue(p.PresharedKey)))
		writeIf(w, p.Endpoint != nil, "endpoint=%s\n", p.Endpoint.String())
		writeIf(w, p.PersistentKeepaliveInterval != nil, "persistent_keepalive_interval=%d\n", int(valueOrZeroValue(p.PersistentKeepaliveInterval).Seconds()))
		writeIf(w, p.ReplaceAllowedIPs, "replace_allowed_ips=true\n")
		for _, ip := range p.AllowedIPs {
			mustWritef(w, "allowed_ip=%s\n", ip.String())
		}
	}
}

func writeIf(w io.Writer, cond bool, format string, args ...any) {
	if cond {
		mustWritef(w, format, args...)
	}
}

func mustWritef(w io.Writer, format string, args ...any) {
	_, err := fmt.Fprintf(w, format, args...)
	if err != nil {
		panic(err)
	}
}

func valueOrZeroValue[Value any](pointer *Value) Value {
	if pointer == nil {
		var zeroValue Value
		return zeroValue
	}
	return *pointer
}

func keyToHex(key wgtypes.Key) string {
	return hex.EncodeToString(key[:])
}
