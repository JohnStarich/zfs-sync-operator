package wireguard

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gvisor.dev/gvisor/pkg/tcpip"
)

func TestEncodableStats(t *testing.T) {
	t.Parallel()
	stats := tcpip.Stats{}.FillIn()
	stats.TCP.Timeouts.Increment()

	encodable, err := encodableStats(stats)
	require.NoError(t, err)
	encoded, err := json.Marshal(encodable)
	require.NoError(t, err)
	encodedStr := string(encoded)
	assert.Contains(t, encodedStr, `"Timeouts":1`)
	// Ensure empty data is omitted
	assert.NotContains(t, encodedStr, "0")
	assert.NotContains(t, encodedStr, "UDP")
}
