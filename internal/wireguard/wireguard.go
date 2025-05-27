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

	"golang.zx2c4.com/wireguard/tun/netstack"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
)

type Config struct {
	PrivateKey    []byte
	PresharedKey  *[]byte
	PeerPublicKey []byte
	PeerAddr      *netip.AddrPort
	ListenPort    int
	LogHandler    slog.Handler
}

func Connect(ctx context.Context, localAddress netip.Addr, config Config) (*netstack.Net, error) {
	var listenPort *int
	if config.ListenPort != 0 {
		listenPort = toPointer(config.ListenPort)
	}
	var presharedKey *wgtypes.Key
	if config.PresharedKey != nil {
		presharedKey = toPointer(wgtypes.Key(*config.PresharedKey))
	}
	var endpoint *net.UDPAddr
	if config.PeerAddr != nil {
		endpoint = net.UDPAddrFromAddrPort(*config.PeerAddr)
	}
	wgConfig := wgtypes.Config{
		PrivateKey:   toPointer(wgtypes.Key(config.PrivateKey)),
		ListenPort:   listenPort,
		ReplacePeers: true,
		Peers: []wgtypes.PeerConfig{
			{
				PublicKey:         wgtypes.Key(config.PeerPublicKey),
				PresharedKey:      presharedKey,
				Endpoint:          endpoint,
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
	iface := NewInterface(logger)
	device, dialer, err := iface.Start(ctx, localAddress)
	if err != nil {
		return nil, err
	}
	go func() {
		err := iface.Wait()
		if err != nil {
			fmt.Println("Failed on wait for interface:", err)
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
		fmt.Fprintf(w, "public_key=%s\n", keyToHex(p.PublicKey))
		writeIf(w, p.Remove, "remove=true\n")
		writeIf(w, p.UpdateOnly, "update_only=true\n")
		writeIf(w, p.PresharedKey != nil, "preshared_key=%s\n", keyToHex(valueOrZeroValue(p.PresharedKey)))
		writeIf(w, p.Endpoint != nil, "endpoint=%s\n", p.Endpoint.String())
		writeIf(w, p.PersistentKeepaliveInterval != nil, "persistent_keepalive_interval=%d\n", int(valueOrZeroValue(p.PersistentKeepaliveInterval).Seconds()))
		writeIf(w, p.ReplaceAllowedIPs, "replace_allowed_ips=true\n")
		for _, ip := range p.AllowedIPs {
			fmt.Fprintf(w, "allowed_ip=%s\n", ip.String())
		}
	}
}

func writeIf(w io.Writer, cond bool, format string, args ...any) {
	if cond {
		fmt.Fprintf(w, format, args...)
	}
}

func toPointer[Value any](value Value) *Value {
	return &value
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
