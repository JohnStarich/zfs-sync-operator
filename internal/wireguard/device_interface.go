package wireguard

import (
	"context"
	"net/netip"

	"golang.zx2c4.com/wireguard/conn"
	wgdevice "golang.zx2c4.com/wireguard/device"
	"golang.zx2c4.com/wireguard/tun/netstack"
)

// DeviceInterface starts, runs, and stops a WireGuard device's interface in userspace.
type DeviceInterface struct {
	done   chan struct{}
	runErr error
}

// NewInterface returns a new [DeviceInterface] for the given interface name
func NewInterface() *DeviceInterface {
	return &DeviceInterface{
		done: make(chan struct{}),
	}
}

// Starts and runs the interface until the context is canceled.
// Use [Wait] to view any errors encountered while running.
func (i *DeviceInterface) Start(ctx context.Context) (*wgdevice.Device, *netstack.Net, error) {
	tunDevice, tunNet, err := netstack.CreateNetTUN(
		[]netip.Addr{
			// TODO do we need these? looks like this is only for traffic returning to us
			netip.MustParseAddr("10.3.0.28"),
			netip.MustParseAddr("10.3.0.29"),
			netip.MustParseAddr("1.1.1.1"),
			netip.MustParseAddr("1.0.0.1"),
		},
		[]netip.Addr{
			// Cloudflare DNS
			netip.MustParseAddr("1.1.1.1"),
			netip.MustParseAddr("1.0.0.1"),
		},
		wgdevice.DefaultMTU)
	if err != nil {
		return nil, nil, err
	}

	const maxDeviceErrors = 2
	logger := newLastErrorsLogger(maxDeviceErrors, "wireguard device")
	device := wgdevice.NewDevice(tunDevice, conn.NewDefaultBind(), &wgdevice.Logger{
		Verbosef: wgdevice.DiscardLogf,
		Errorf:   logger.LogError,
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
