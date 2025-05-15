package wireguard

import (
	"context"
	"crypto/rand"
	"fmt"
	"net"

	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
	"golang.zx2c4.com/wireguard/wgctrl"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
)

type Config struct {
	PrivateKey    string
	PresharedKey  *string
	PeerPublicKey string
	PeerIP        string
	PeerPort      int
	AllowedIPs    []net.IPNet
}

func Connect(ctx context.Context, config Config) (returnedErr error) {
	defer func() { returnedErr = errors.WithStack(returnedErr) }()
	interfaceName := mustNewInterfaceName()
	iface := NewInterface(interfaceName)
	err := iface.Start(ctx)
	if err != nil {
		return err
	}
	go func() {
		err := iface.Wait()
		if err != nil {
			fmt.Println(err)
		}
	}()

	privateKey, err := wgtypes.ParseKey(config.PrivateKey)
	if err != nil {
		return err
	}
	peerPublicKey, err := wgtypes.ParseKey(config.PeerPublicKey)
	if err != nil {
		return err
	}
	var presharedKey *wgtypes.Key
	if config.PresharedKey != nil {
		key, presharedKeyErr := wgtypes.ParseKey(*config.PresharedKey)
		if presharedKeyErr != nil {
			return presharedKeyErr
		}
		presharedKey = &key
	}
	peerIP := net.ParseIP(config.PeerIP)
	if peerIP == nil {
		return errors.Errorf("invalid IP: %q", config.PeerIP)
	}
	peer := wgtypes.PeerConfig{
		PublicKey:    peerPublicKey,
		PresharedKey: presharedKey,
		AllowedIPs:   config.AllowedIPs,
		Endpoint:     &net.UDPAddr{IP: peerIP, Port: config.PeerPort},
	}
	client, err := wgctrl.New()
	if err != nil {
		return err
	}
	defer client.Close()
	fmt.Println("iface name:", interfaceName)
	err = client.ConfigureDevice(interfaceName, wgtypes.Config{
		PrivateKey:   &privateKey,
		ReplacePeers: true,
		Peers:        []wgtypes.PeerConfig{peer},
	})
	if err != nil {
		return err
	}
	/*
		TODO

		- configure a source and destination
		- test connection between them, stats maybe?
		- maybe move to separate container to facilitate and isolate the connections
	*/
	return nil
}

// mustNewInterfaceName returns a new random interface name to reduce likelihood of a collision and interface creation errors.
// Must comply to unix interface name length limits.
func mustNewInterfaceName() string {
	const maxLength = unix.IFNAMSIZ - 1
	const prefix = "wg-"
	const hexDigitsPerByte = 2
	buf := make([]byte, (maxLength-len(prefix))/hexDigitsPerByte)
	_, err := rand.Read(buf)
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("%s%x", prefix, buf)
}
