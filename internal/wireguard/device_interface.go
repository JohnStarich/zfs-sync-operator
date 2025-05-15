package wireguard

import (
	"context"
	"net"
	"slices"

	"golang.zx2c4.com/wireguard/conn"
	wgdevice "golang.zx2c4.com/wireguard/device"
	"golang.zx2c4.com/wireguard/ipc"
	"golang.zx2c4.com/wireguard/tun"
)

// DeviceInterface starts, runs, and stops a WireGuard device's interface in userspace.
type DeviceInterface struct {
	name   string
	done   chan struct{}
	runErr error
}

// NewInterface returns a new [DeviceInterface] for the given interface name
func NewInterface(name string) *DeviceInterface {
	return &DeviceInterface{
		name: name,
		done: make(chan struct{}),
	}
}

// Starts and runs the interface until the context is canceled.
// Use [Wait] to view any errors encountered while running.
func (i *DeviceInterface) Start(ctx context.Context) (returnedErr error) {
	var tearDownOperations []func() error
	defer func() { // tear down if Start fails
		if returnedErr != nil {
			for _, fn := range slices.Backward(tearDownOperations) {
				_ = fn()
			}
		}
	}()

	tunDevice, err := tun.CreateTUN(i.name, wgdevice.DefaultMTU)
	if err != nil {
		return err
	}
	//if realInterfaceName, err := tunDevice.Name(); err == nil {
	//name = realInterfaceName
	//}
	tearDownOperations = append(tearDownOperations, tunDevice.Close)

	const maxDeviceErrors = 2
	logger := newLastErrorsLogger(maxDeviceErrors, "wireguard device")
	device := wgdevice.NewDevice(tunDevice, conn.NewDefaultBind(), &wgdevice.Logger{
		Verbosef: wgdevice.DiscardLogf,
		Errorf:   logger.LogError,
	})
	tearDownOperations = append(tearDownOperations, func() error {
		device.Close()
		return nil
	})

	socket, err := ipc.UAPIOpen(i.name)
	if err != nil {
		return err
	}
	tearDownOperations = append(tearDownOperations, socket.Close)

	listener, err := ipc.UAPIListen(i.name, socket)
	if err != nil {
		return err
	}
	tearDownOperations = append(tearDownOperations, listener.Close)

	go func() {
		defer close(i.done)
		defer func() {
			for _, fn := range slices.Backward(tearDownOperations) {
				err := fn()
				if i.runErr == nil {
					i.runErr = err
				}
			}
		}()
		i.runErr = run(ctx, logger, device, listener)
	}()
	return nil
}

// Wait blocks until the interface's context is canceled or an error occurs, then after teardown completes.
// Returns the first encountered error, if any.
func (i *DeviceInterface) Wait() error {
	<-i.done
	return i.runErr
}

// TODO look deeper at the HTTP example, may not need to create all of this just to open a socket: https://github.com/WireGuard/wireguard-go/tree/436f7fdc1670df26eee958de464cf5cb0385abec/tun/netstack/examples
// example looks like there's no need for an interface at all
func run(ctx context.Context, logger *lastErrorsLogger, device *wgdevice.Device, listener net.Listener) error {
	errs := make(chan error, 1)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				errs <- err
				return
			}
			go device.IpcHandle(conn)
		}
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errs:
		return err
	case <-device.Wait():
		return logger.LastErrors()
	}
}
