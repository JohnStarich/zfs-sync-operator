package wireguard

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"

	"golang.zx2c4.com/wireguard/device"
	"golang.zx2c4.com/wireguard/tun/netstack"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
)

type Config struct {
	PrivateKey    string
	PresharedKey  *string
	PeerPublicKey string
	PeerIP        string
	PeerPort      int
	AllowedIPs    []net.IPNet
	ListenPort    int
}

func startInterface(ctx context.Context, logger *slog.Logger, localAddress netip.Addr) (*device.Device, *netstack.Net, error) {
	iface := NewInterface(logger)
	device, dialer, err := iface.Start(ctx, localAddress)
	if err != nil {
		return nil, nil, err
	}
	go func() {
		err := iface.Wait()
		if err != nil {
			fmt.Println("Failed on wait for interface:", err)
		}
	}()
	return device, dialer, nil
}

func StartServer(ctx context.Context, privateKey wgtypes.Key, peerPublicKey wgtypes.Key) (*netstack.Net, error) {
	device, dialer, connectErr := startInterface(ctx, slog.Default(), netip.MustParseAddr("192.168.4.29"))
	if connectErr != nil {
		return nil, connectErr
	}
	var configBuffer bytes.Buffer
	writeConfig(&configBuffer, wgtypes.Config{
		PrivateKey: toPointer(privateKey),
		ListenPort: toPointer(58120),
		Peers: []wgtypes.PeerConfig{
			{
				PublicKey: peerPublicKey,
				AllowedIPs: []net.IPNet{
					{IP: net.ParseIP("192.168.4.28"), Mask: net.CIDRMask(32, 32)},
				},
			},
		},
	})
	if err := device.IpcSet(configBuffer.String()); err != nil {
		return nil, err
	}
	if err := device.Up(); err != nil {
		return nil, err
	}
	listener, err := dialer.ListenTCP(&net.TCPAddr{Port: 80})
	if err != nil {
		return nil, err
	}
	server := http.Server{
		Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			fmt.Printf("> %s - %s - %s\n", request.RemoteAddr, request.URL.String(), request.UserAgent())
			io.WriteString(writer, "Hello from userspace TCP!")
		}),
	}
	if err := server.Serve(listener); err != nil {
		return nil, err
	}
	return dialer, nil
}

func StartClient(ctx context.Context, privateKey wgtypes.Key, peerPublicKey wgtypes.Key) (*http.Client, error) {
	device, dialer, connectErr := startInterface(ctx, slog.Default(), netip.MustParseAddr("192.168.4.28"))
	if connectErr != nil {
		return nil, connectErr
	}
	var configBuffer bytes.Buffer
	writeConfig(&configBuffer, wgtypes.Config{
		PrivateKey: toPointer(privateKey),
		Peers: []wgtypes.PeerConfig{
			{
				PublicKey: peerPublicKey,
				Endpoint:  net.UDPAddrFromAddrPort(netip.MustParseAddrPort("127.0.0.1:58120")),
				AllowedIPs: []net.IPNet{
					{IP: net.ParseIP("0.0.0.0"), Mask: net.CIDRMask(0, 32)},
				},
			},
		},
	})
	if err := device.IpcSet(configBuffer.String()); err != nil {
		return nil, err
	}
	if err := device.Up(); err != nil {
		return nil, err
	}
	return &http.Client{
		Transport: &http.Transport{
			DialContext: dialer.DialContext,
		},
	}, nil
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
	return fmt.Sprintf("%x", [wgtypes.KeyLen]byte(key))
}

func mustHexDecode(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}
