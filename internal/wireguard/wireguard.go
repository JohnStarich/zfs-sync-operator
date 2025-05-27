package wireguard

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"

	"github.com/pkg/errors"
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

func Connect(ctx context.Context, config Config) (dialer *netstack.Net, returnedErr error) {
	defer func() { returnedErr = errors.WithStack(returnedErr) }()
	iface := NewInterface()
	device, dialer, err := iface.Start(ctx)
	if err != nil {
		return nil, err
	}
	go func() {
		err := iface.Wait()
		if err != nil {
			fmt.Println(err)
		}
	}()

	privateKey, err := wgtypes.ParseKey(config.PrivateKey)
	if err != nil {
		return nil, err
	}
	peerPublicKey, err := wgtypes.ParseKey(config.PeerPublicKey)
	if err != nil {
		return nil, err
	}
	var presharedKey *wgtypes.Key
	if config.PresharedKey != nil {
		key, presharedKeyErr := wgtypes.ParseKey(*config.PresharedKey)
		if presharedKeyErr != nil {
			return nil, presharedKeyErr
		}
		presharedKey = &key
	}
	var peers []wgtypes.PeerConfig
	if config.PeerIP != "" {
		peerIP := net.ParseIP(config.PeerIP)
		if peerIP == nil {
			return nil, errors.Errorf("invalid IP: %q", config.PeerIP)
		}
		peers = []wgtypes.PeerConfig{
			{
				PublicKey:    peerPublicKey,
				PresharedKey: presharedKey,
				AllowedIPs:   config.AllowedIPs,
				Endpoint:     &net.UDPAddr{IP: peerIP, Port: config.PeerPort},
			},
		}
	}
	var listenPort *int
	if config.ListenPort != 0 {
		listenPort = &config.ListenPort
	}
	wgConfig := wgtypes.Config{
		PrivateKey:   &privateKey,
		ReplacePeers: true,
		ListenPort:   listenPort,
		Peers:        peers,
	}

	var buf bytes.Buffer
	writeConfig(&buf, wgConfig)
	if err := device.IpcSetOperation(&buf); err != nil {
		return nil, err
	}

	/*
		TODO

		- configure a source and destination
		- test connection between them, stats maybe?
		- maybe move to separate container to facilitate and isolate the connections
	*/
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

func valueOrZeroValue[Value any](valuePointer *Value) Value {
	if valuePointer == nil {
		var zeroValue Value
		return zeroValue
	}
	return *valuePointer
}

func keyToHex(key wgtypes.Key) string {
	return fmt.Sprintf("%x\n", [wgtypes.KeyLen]byte(key))
}
