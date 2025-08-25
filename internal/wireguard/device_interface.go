package wireguard

import (
	"context"
	"fmt"
	"log/slog"
	"net/netip"

	"github.com/johnstarich/zfs-sync-operator/internal/netstack"
	"golang.zx2c4.com/wireguard/conn"
	wgdevice "golang.zx2c4.com/wireguard/device"
)

// DeviceInterface starts, runs, and stops a WireGuard device's interface in userspace.
type DeviceInterface struct {
	done         chan struct{}
	localAddress netip.Addr
	logger       *slog.Logger
	runErr       error
}

// newInterface returns a new [DeviceInterface] for the given interface name
func newInterface(logger *slog.Logger, localAddress netip.Addr) *DeviceInterface {
	return &DeviceInterface{
		done:         make(chan struct{}),
		localAddress: localAddress,
		logger:       logger,
	}
}

// Start starts and runs the interface until the context is canceled.
// Use [Wait] to view any errors encountered while running.
func (i *DeviceInterface) Start(ctx context.Context) (*wgdevice.Device, *netstack.Net, error) {
	const (
		defaultMTU        = 1500
		wireguardOverhead = defaultMTU - wgdevice.DefaultMTU
		// OKD and OpenShift can use OVN-Kubernetes for cluster networking, which has a sizable overhead.
		// Carve that out for everyone with this, but this could probably be discoverable or configurable in the future.
		// https://docs.redhat.com/en/documentation/openshift_container_platform/4.19/html/advanced_networking/changing-cluster-network-mtu#mtu-value-selection_changing-cluster-network-mtu
		ovnKubernetesOverhead = 100
	)
	tunNet, err := netstack.New([]netip.Addr{i.localAddress}, defaultMTU-wireguardOverhead-ovnKubernetesOverhead)
	if err != nil {
		return nil, nil, err
	}

	const maxDeviceErrors = 2
	logger := newLastErrorsLogger(maxDeviceErrors, "wireguard device")
	device := wgdevice.NewDevice(tunNet, conn.NewDefaultBind(), &wgdevice.Logger{
		Verbosef: func(format string, args ...any) {
			i.logger.Debug(fmt.Sprintf(format, args...))
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
