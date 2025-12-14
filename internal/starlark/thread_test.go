package starlark

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.starlark.net/starlark"
)

func TestThreadPool_GetPut(t *testing.T) {
	pool := NewThreadPool(5)

	// Get a thread
	thread := pool.Get("test1")
	require.NotNil(t, thread, "Get returned nil")
	assert.Equal(t, "test1", thread.Name, "thread.Name")

	// Return it
	pool.Put(thread)
	assert.Equal(t, 1, pool.Size(), "pool size after put")

	// Get it again - should be reused
	thread2 := pool.Get("test2")
	assert.Equal(t, 0, pool.Size(), "pool size after get")
	assert.Equal(t, "test2", thread2.Name, "thread.Name after reuse")
}

func TestThreadPool_MaxSize(t *testing.T) {
	pool := NewThreadPool(2)

	// Create and return 3 threads
	threads := make([]*starlark.Thread, 3)
	for i := 0; i < 3; i++ {
		threads[i] = pool.Get("test")
	}

	for _, thread := range threads {
		pool.Put(thread)
	}

	// Pool should only have 2 threads (max size)
	assert.Equal(t, 2, pool.Size(), "pool size should be max (2)")
}

func TestThreadPool_DefaultSize(t *testing.T) {
	pool := NewThreadPool(0) // Should use default

	// Should be able to store at least some threads
	for i := 0; i < 5; i++ {
		pool.Put(pool.Get("test"))
	}

	assert.NotEqual(t, 0, pool.Size(), "pool size should not be 0 after puts")
}

func TestThreadPool_Concurrent(t *testing.T) {
	pool := NewThreadPool(10)
	var wg sync.WaitGroup

	// Concurrently get and put threads
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			thread := pool.Get("concurrent")
			// Simulate some work
			pool.Put(thread)
		}(i)
	}

	wg.Wait()

	// Pool should have some threads returned
	assert.LessOrEqual(t, pool.Size(), 10, "pool size should not exceed max of 10")
}

func TestParallelExecutor_Execute(t *testing.T) {
	globals := starlark.StringDict{
		"x": starlark.MakeInt(10),
		"y": starlark.MakeInt(20),
	}

	executor := NewParallelExecutor(5, globals)

	tasks := []EvalTask{
		{Name: "task1", Expr: "x + 1"},
		{Name: "task2", Expr: "y + 2"},
		{Name: "task3", Expr: "x + y"},
	}

	results := executor.Execute(tasks)

	require.Len(t, results, 3, "expected 3 results")

	// Check results (order is preserved)
	expected := []int64{11, 22, 30}
	for i, result := range results {
		require.NoError(t, result.Error, "task %d error", i)
		val, _ := result.Value.(starlark.Int).Int64()
		assert.Equal(t, expected[i], val, "task %d result", i)
	}
}

func TestParallelExecutor_ExecuteWithErrors(t *testing.T) {
	globals := starlark.StringDict{}
	executor := NewParallelExecutor(2, globals)

	tasks := []EvalTask{
		{Name: "valid", Expr: "1 + 1"},
		{Name: "invalid", Expr: "undefined_var"},
	}

	results := executor.Execute(tasks)

	require.Len(t, results, 2, "expected 2 results")

	// First should succeed
	assert.NoError(t, results[0].Error, "task 0 should succeed")

	// Second should fail
	assert.Error(t, results[1].Error, "task 1 should fail with undefined variable")
}
