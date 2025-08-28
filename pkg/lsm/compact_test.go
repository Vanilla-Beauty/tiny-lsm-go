package lsm

import (
	"fmt"
	"os"
	"testing"
	"tiny-lsm-go/pkg/config"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBasicCompaction(t *testing.T) {
	dir := "test_data"

	defer func() {
		err := os.RemoveAll(dir)
		require.NoError(t, err)
	}()

	test_num := 1200
	{
		cfg := config.DefaultConfig()
		cfg.LSM.Core.SSTLevelRatio = 4 // 4 SSTs trigger compaction
		cfg.LSM.Core.PerMemSizeLimit = 10400
		engine, err := NewEngine(cfg, dir)
		require.NoError(t, err)
		dir = engine.dataDir
		engine.flushAndCompactByHand = true

		for i := 0; i < test_num; i++ {
			key := "key" + fmt.Sprintf("%04d", i) // Zero-padded for consistent ordering, len=7
			val := "val" + fmt.Sprintf("%04d", i) // Zero-padded for consistent ordering, len=7

			engine.Put(key, val)

			if i == 0 {
				continue
			}
			if i%10 == 0 {
				del_key := "key" + fmt.Sprintf("%04d", i-10) // Zero-padded for consistent ordering
				engine.Delete(del_key)
			}
			if i%100 == 0 {
				// Force flush 12000/100=120 level-0 SSTs
				engine.Flush()
			}
			if i%400 == 0 {
				// Force compact
				engine.ForceCompact()
			}
		}
		// Loop only goes to 1199, so 1190 (1200-10) needs to be manually deleted
		del_key := "key" + fmt.Sprintf("%04d", 1190) // Zero-padded for consistent ordering
		engine.Delete(del_key)
		engine.Flush()
		engine.ForceCompact()

		engine.Close()
	}

	cfg := config.DefaultConfig()
	newEngine, err := NewEngine(cfg, dir)
	require.NoError(t, err)
	require.NotNil(t, newEngine)

	for i := 0; i < test_num; i++ {
		key := "key" + fmt.Sprintf("%04d", i)          // Zero-padded for consistent ordering
		expected_val := "val" + fmt.Sprintf("%04d", i) // Zero-padded for consistent ordering
		val, found, err := newEngine.Get(key)
		assert.NoError(t, err)

		if i%10 == 0 {
			assert.False(t, found, "Expected key %s to be deleted", key)
		} else {
			assert.True(t, found, "Expected key %s to be found", key)
			assert.Equal(t, expected_val, val, "Value mismatch for key %s", key)
		}
	}

	newEngine.Close()
}
