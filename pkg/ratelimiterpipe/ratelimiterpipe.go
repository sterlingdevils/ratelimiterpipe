package ratelimiterpipe

import (
	"context"
	"sync"

	"github.com/sterlingdevils/pipelines"
	"golang.org/x/time/rate"
)

const (
	CHANSIZE = 0
)

type RateLimiterPipe[T pipelines.SizerDater] struct {
	limit *rate.Limiter

	ctx context.Context
	can context.CancelFunc

	inchan  chan T
	outchan chan T

	pl pipelines.Pipeline[T]
	wg sync.WaitGroup
}

// InChan
func (r *RateLimiterPipe[T]) InChan() chan<- T {
	return r.inchan
}

// OutChan
func (r *RateLimiterPipe[T]) OutChan() <-chan T {
	return r.outchan
}

// PipelineChan returns a R/W channel that is used for pipelining
func (r *RateLimiterPipe[T]) PipelineChan() chan T {
	return r.outchan
}

func (r *RateLimiterPipe[_]) Close() {
	// If we pipelined then call Close the input pipeline
	if r.pl != nil {
		r.pl.Close()
	}

	// Cancel our context
	r.can()

	// Wait for us to be done
	r.wg.Wait()
}

func (r *RateLimiterPipe[_]) SetLimit(l rate.Limit) {
	r.limit.SetLimit(l)
}

func (r *RateLimiterPipe[_]) SetBurst(n int) {
	r.limit.SetBurst(n)
}

func (r *RateLimiterPipe[_]) mainloop() {
	defer r.wg.Done()
	defer close(r.outchan)

	for {
		select {
		case t, more := <-r.inchan:
			if !more { // if the channel is closed, then we are done
				return
			}
			err := r.limit.WaitN(r.ctx, t.Size())
			if err != nil {
				continue
			}
			r.outchan <- t
		case <-r.ctx.Done():
			return
		}
	}
}

func NewWithChannel[T pipelines.SizerDater](rLimit rate.Limit, bLimit int, in chan T) *RateLimiterPipe[T] {
	con, cancel := context.WithCancel(context.Background())
	r := RateLimiterPipe[T]{
		limit:   rate.NewLimiter(rLimit, bLimit),
		ctx:     con,
		can:     cancel,
		inchan:  in,
		outchan: make(chan T, CHANSIZE)}

	r.wg.Add(1)
	go r.mainloop()

	return &r
}

func NewWithPipeline[T pipelines.SizerDater](rLimit rate.Limit, bLimit int, p pipelines.Pipeline[T]) *RateLimiterPipe[T] {
	r := NewWithChannel(rLimit, bLimit, p.PipelineChan())

	r.pl = p
	return r
}

func New[T pipelines.SizerDater](rLimit rate.Limit, bLimit int) *RateLimiterPipe[T] {
	return NewWithChannel(rLimit, bLimit, make(chan T, CHANSIZE))
}
