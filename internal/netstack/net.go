// Package netstack creates userland network stacks. Useful for avoiding kernel module privilege requirements.
//
// This was inspired by the great work in wireguard-go, coder, and tailscale.
package netstack

import (
	"context"
	pkgnet "net"
	"net/netip"
	"os"
	"syscall"

	"github.com/johnstarich/zfs-sync-operator/internal/pointer"
	"github.com/pkg/errors"
	"golang.zx2c4.com/wireguard/tun"
	gvisorBuffer "gvisor.dev/gvisor/pkg/buffer"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/link/channel"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv6"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
)

// Net is a userland network stack
type Net struct {
	channelEndpoint     *channel.Endpoint
	events              chan tun.Event
	incomingPacket      chan *stack.PacketBuffer
	maxTransmissionUnit uint32
	notifyHandle        *channel.NotificationHandle
	stack               *stack.Stack
}

var _ tun.Device = &Net{}

const nicID = 1

// New returns a new userland network stack.
//
// Useful for avoiding kernel module privilege requirements to manage TUN devices, like with WireGuard.
func New(localAddresses []netip.Addr, maxTransmissionUnit uint32) (*Net, error) {
	opts := stack.Options{
		NetworkProtocols: []stack.NetworkProtocolFactory{ipv4.NewProtocol, ipv6.NewProtocol},
		TransportProtocols: []stack.TransportProtocolFactory{
			tcp.NewProtocolCUBIC, // Use cubic to less aggressively increase traffic and avoid overwhelming the link. https://github.com/google/gvisor/pull/10287
		},
		HandleLocal: true,
	}
	const channelEndpointSize = 1 << 10
	net := &Net{
		channelEndpoint:     channel.New(channelEndpointSize, maxTransmissionUnit, ""),
		stack:               stack.New(opts),
		events:              make(chan tun.Event, 10),
		incomingPacket:      make(chan *stack.PacketBuffer),
		maxTransmissionUnit: maxTransmissionUnit,
	}
	for _, option := range []tcpip.SettableTransportProtocolOption{
		pointer.Of(tcpip.TCPSACKEnabled(true)),
		pointer.Of(tcpip.TCPRecovery(0)), // Disable recovery to save some CPU cycles in userspace, defer to kernel and network level recovery.
	} {
		if err := net.stack.SetTransportProtocolOption(tcp.ProtocolNumber, option); err != nil {
			return nil, errors.Errorf("failed to set TCP transport option %v: %v", option, err)
		}
	}
	net.notifyHandle = net.channelEndpoint.AddNotify(net)
	if err := net.stack.CreateNIC(nicID, net.channelEndpoint); err != nil {
		return nil, errors.Errorf("failed to create NIC: %v", err)
	}

	var hasV4, hasV6 bool
	for _, addr := range localAddresses {
		protocolNumber := ipv6.ProtocolNumber
		if addr.Is4() {
			protocolNumber = ipv4.ProtocolNumber
		}
		err := net.stack.AddProtocolAddress(nicID, tcpip.ProtocolAddress{
			Protocol:          protocolNumber,
			AddressWithPrefix: tcpip.AddrFromSlice(addr.AsSlice()).WithPrefix(),
		}, stack.AddressProperties{})
		if err != nil {
			return nil, errors.Errorf("failed to add protocol address %v: %v", addr, err)
		}
		hasV4 = hasV4 || addr.Is4()
		hasV6 = hasV6 || addr.Is6()
	}
	if hasV4 {
		net.stack.AddRoute(tcpip.Route{Destination: header.IPv4EmptySubnet, NIC: nicID})
	}
	if hasV6 {
		net.stack.AddRoute(tcpip.Route{Destination: header.IPv6EmptySubnet, NIC: nicID})
	}

	net.events <- tun.EventUp
	return net, nil
}

// BatchSize implements [tun.Device]
func (n *Net) BatchSize() int { return 1 }

// Events implements [tun.Device]
func (n *Net) Events() <-chan tun.Event { return n.events }

// File implements [tun.Device]
func (n *Net) File() *os.File { return nil }

// MTU implements [tun.Device]
func (n *Net) MTU() (int, error) { return int(n.maxTransmissionUnit), nil }

// Name implements [tun.Device]
func (n *Net) Name() (string, error) { return "go", nil }

func (n *Net) Read(buffers [][]byte, sizes []int, offset int) (int, error) {
	packet, ok := <-n.incomingPacket
	if !ok {
		return 0, os.ErrClosed
	}
	defer packet.DecRef()

	buffer := packet.ToBuffer()
	bufferReader := buffer.AsBufferReader()
	bytes, err := bufferReader.Read(buffers[0][offset:])
	sizes[0] = bytes
	return 1, err
}

func (n *Net) Write(buffers [][]byte, offset int) (int, error) {
	for index, buffer := range buffers {
		err := n.write(buffer[offset:])
		if err != nil {
			return index, err
		}
	}
	return len(buffers), nil
}

func (n *Net) write(packet []byte) error {
	if len(packet) == 0 {
		return nil
	}

	packetBuffer := stack.NewPacketBuffer(stack.PacketBufferOptions{
		Payload: gvisorBuffer.MakeWithData(packet),
	})
	switch header.IPVersion(packet) {
	case header.IPv4Version:
		n.channelEndpoint.InjectInbound(header.IPv4ProtocolNumber, packetBuffer)
		return nil
	case header.IPv6Version:
		n.channelEndpoint.InjectInbound(header.IPv6ProtocolNumber, packetBuffer)
		return nil
	default:
		return syscall.EAFNOSUPPORT
	}
}

// WriteNotify receives notifications from its attached NIC when packets are ready to read from the endpoint.
// Attached network devices, like WireGuard read these as local inbound traffic to then be sent over the VPN.
func (n *Net) WriteNotify() {
	pkt := n.channelEndpoint.Read()
	if pkt == nil {
		return
	}
	n.incomingPacket <- pkt
}

// Close cleans up and stops the network stack
func (n *Net) Close() error {
	n.stack.RemoveNIC(nicID)
	n.stack.Close()
	n.channelEndpoint.RemoveNotify(n.notifyHandle)
	n.channelEndpoint.Close()
	close(n.events)
	close(n.incomingPacket)
	return nil
}

// Stats returns the network stack's statistics
func (n *Net) Stats() tcpip.Stats { return n.stack.Stats() }

func asFullAddressAndProto(endpoint netip.AddrPort) (tcpip.FullAddress, tcpip.NetworkProtocolNumber) {
	protoNumber := ipv6.ProtocolNumber
	if endpoint.Addr().Is4() {
		protoNumber = ipv4.ProtocolNumber
	}
	return tcpip.FullAddress{
		NIC:  nicID,
		Addr: tcpip.AddrFromSlice(endpoint.Addr().AsSlice()),
		Port: endpoint.Port(),
	}, protoNumber
}

// ListenTCPAddrPort returns a listener for connections on the given address and port
func (n *Net) ListenTCPAddrPort(addr netip.AddrPort) (*gonet.TCPListener, error) {
	fullAddress, protocolNumber := asFullAddressAndProto(addr)
	return gonet.ListenTCP(n.stack, fullAddress, protocolNumber)
}

// DialContext implements the same interface as net.Dialer to dial TCP connections
func (n *Net) DialContext(ctx context.Context, network, address string) (pkgnet.Conn, error) {
	if network != "tcp" {
		return nil, errors.New("only 'tcp' is supported")
	}
	addrPort, err := netip.ParseAddrPort(address)
	if err != nil {
		return nil, &pkgnet.OpError{Op: "dial", Err: err}
	}
	fullAddress, protocolNumber := asFullAddressAndProto(addrPort)
	return gonet.DialContextTCP(ctx, n.stack, fullAddress, protocolNumber)
}
