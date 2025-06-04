package ssh

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func WaitUntilTCPDialable(tb testing.TB, address string, dialContext func(ctx context.Context, network, address string) (net.Conn, error)) {
	const tick = 10 * time.Millisecond
	require.EventuallyWithT(tb, func(collect *assert.CollectT) {
		ctx, cancel := context.WithTimeout(context.Background(), tick)
		defer cancel()
		conn, err := dialContext(ctx, "tcp", address)
		if assert.NoError(collect, err) {
			const writeDeadline = 100 * time.Millisecond
			assert.NoError(collect, conn.SetWriteDeadline(time.Now().Add(writeDeadline)))
			_, err := conn.Write([]byte("boo!"))
			assert.NoError(collect, err)
			assert.NoError(collect, conn.Close())
		}
	}, 5*time.Second, tick)
}
