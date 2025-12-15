package starlark

import (
	"sync"

	"go.starlark.net/starlark"
)

// ThreadPool manages a pool of Starlark threads for parallel execution.
// This is useful for rendering multiple templates concurrently.
type ThreadPool struct {
	mu      sync.Mutex
	threads []*starlark.Thread
	maxSize int
}

// NewThreadPool creates a new thread pool with the specified maximum size.
func NewThreadPool(maxSize int) *ThreadPool {
	if maxSize <= 0 {
		maxSize = 10 // default pool size
	}
	return &ThreadPool{
		threads: make([]*starlark.Thread, 0, maxSize),
		maxSize: maxSize,
	}
}

// Get retrieves a thread from the pool or creates a new one.
// The thread name is used for error reporting.
func (p *ThreadPool) Get(name string) *starlark.Thread {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.threads) > 0 {
		thread := p.threads[len(p.threads)-1]
		p.threads = p.threads[:len(p.threads)-1]
		thread.Name = name
		return thread
	}

	return &starlark.Thread{
		Name: name,
		Print: func(_ *starlark.Thread, _ string) {
			// No-op for template execution
		},
	}
}

// Put returns a thread to the pool for reuse.
// If the pool is full, the thread is discarded.
func (p *ThreadPool) Put(thread *starlark.Thread) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.threads) < p.maxSize {
		// Clear any state that might leak between uses
		thread.Name = ""
		p.threads = append(p.threads, thread)
	}
}

// Size returns the current number of threads in the pool.
func (p *ThreadPool) Size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.threads)
}

// ExecuteContext represents the execution context for a single template rendering.
type ExecuteContext struct {
	Thread  *starlark.Thread
	Globals starlark.StringDict
}

// ParallelExecutor executes multiple template renders in parallel.
type ParallelExecutor struct {
	pool    *ThreadPool
	globals starlark.StringDict
}

// NewParallelExecutor creates a new parallel executor with shared globals.
func NewParallelExecutor(maxConcurrency int, globals starlark.StringDict) *ParallelExecutor {
	return &ParallelExecutor{
		pool:    NewThreadPool(maxConcurrency),
		globals: globals,
	}
}

// Execute runs multiple evaluations in parallel and collects results.
func (e *ParallelExecutor) Execute(tasks []EvalTask) []EvalResult {
	results := make([]EvalResult, len(tasks))
	var wg sync.WaitGroup

	for i, task := range tasks {
		wg.Add(1)
		go func(idx int, t EvalTask) {
			defer wg.Done()

			thread := e.pool.Get(t.Name)
			defer e.pool.Put(thread)

			result, err := starlark.Eval(thread, t.Name, t.Expr, e.globals) //nolint:staticcheck // SA1019: will migrate to EvalOptions later
			results[idx] = EvalResult{
				Name:  t.Name,
				Value: result,
				Error: err,
			}
		}(i, task)
	}

	wg.Wait()
	return results
}

// EvalTask represents a single evaluation task.
type EvalTask struct {
	Name string // Identifier for this task (used for error reporting)
	Expr string // Starlark expression to evaluate
}

// EvalResult represents the result of an evaluation task.
type EvalResult struct {
	Name  string
	Value starlark.Value
	Error error
}
