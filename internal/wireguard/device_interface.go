package wireguard

import (
	"context"
	"fmt"
	"log/slog"
	"net/netip"

	"golang.zx2c4.com/wireguard/conn"
	wgdevice "golang.zx2c4.com/wireguard/device"
	"golang.zx2c4.com/wireguard/tun/netstack"
)

// DeviceInterface starts, runs, and stops a WireGuard device's interface in userspace.
type DeviceInterface struct {
	dnsAddresses []netip.Addr
	done         chan struct{}
	localAddress netip.Addr
	logger       *slog.Logger
	runErr       error
}

// NewInterface returns a new [DeviceInterface] for the given interface name
func NewInterface(logger *slog.Logger, localAddress netip.Addr, dnsAddresses []netip.Addr) *DeviceInterface {
	if len(dnsAddresses) == 0 {
		// CloudFlare DNS
		dnsAddresses = []netip.Addr{
			netip.MustParseAddr("1.0.0.1"),
			netip.MustParseAddr("1.1.1.1"),
		}
	}
	return &DeviceInterface{
		dnsAddresses: dnsAddresses,
		done:         make(chan struct{}),
		localAddress: localAddress,
		logger:       logger,
	}
}

// Starts and runs the interface until the context is canceled.
// Use [Wait] to view any errors encountered while running.
func (i *DeviceInterface) Start(ctx context.Context) (*wgdevice.Device, *netstack.Net, error) {
	tunDevice, tunNet, err := netstack.CreateNetTUN([]netip.Addr{i.localAddress}, i.dnsAddresses, wgdevice.DefaultMTU)
	if err != nil {
		return nil, nil, err
	}

	const maxDeviceErrors = 2
	logger := newLastErrorsLogger(maxDeviceErrors, "wireguard device")
	device := wgdevice.NewDevice(tunDevice, conn.NewDefaultBind(), &wgdevice.Logger{
		Verbosef: func(format string, args ...any) {
			i.logger.Info(fmt.Sprintf(format, args...))
		},
		Errorf: func(format string, args ...any) {
			i.logger.Error(fmt.Sprintf(format, args...))
			logger.LogError(format, args...)
		},
	})
	go func() {
		defer close(i.done)
		defer device.Close()
		select {
		case <-ctx.Done():
			i.runErr = ctx.Err()
		case <-device.Wait():
			i.runErr = logger.LastErrors()
		}
	}()
	return device, tunNet, err
}

// Wait blocks until the interface's context is canceled or an error occurs, then after teardown completes.
// Returns the first encountered error, if any.
func (i *DeviceInterface) Wait() error {
	<-i.done
	return i.runErr
}
