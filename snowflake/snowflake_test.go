package snowflake

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSnowFlake(t *testing.T) {
	node := NewNode()

	var (
		wg     sync.WaitGroup
		worker = 1000
		idMap  = make(map[int64]bool)
		mu     sync.RWMutex
	)

	for i := 0; i <= worker; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ii := 0; ii <= 1000; ii++ {
				id, err := node.NextId()
				require.NoError(t, err)
				fmt.Println(id)

				mu.Lock()
				if _, exist := idMap[id.Int64()]; exist {
					t.Error(errors.New("duplicate id"))
				}
				idMap[id.Int64()] = true
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
}

func BenchmarkNodeNextId(b *testing.B) {
	node := NewNode()
	for i := 0; i <= b.N; i++ {
		node.NextId()
	}
}
