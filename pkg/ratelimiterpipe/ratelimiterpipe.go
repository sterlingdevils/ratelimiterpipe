package ratelimiterpipe

import (
	"context"

	"github.com/sterlingdevils/pipelines/pkg/pipeline"
	"golang.org/x/time/rate"
)

type Sizeable interface {
	Size() int
}

const (
	CHANSIZE = 0
)

type RateLimiterPipe[T Sizeable] struct {
	limit *rate.Limiter

	ctx context.Context
	can context.CancelFunc

	inchan  chan T
	outchan chan T

	pl pipeline.Pipelineable[T]
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
}

func (r *RateLimiterPipe[_]) SetLimit(l rate.Limit) {
	r.limit.SetLimit(l)
}

func (r *RateLimiterPipe[_]) SetBurst(n int) {
	r.limit.SetBurst(n)
}

func (r *RateLimiterPipe[_]) mainloop() {
	defer close(r.outchan)

	for {
		select {
		case t := <-r.inchan:
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

func NewWithChannel[T Sizeable](rLimit rate.Limit, bLimit int, in chan T) (*RateLimiterPipe[T], error) {
	con, cancel := context.WithCancel(context.Background())
	r := RateLimiterPipe[T]{
		limit:   rate.NewLimiter(rLimit, bLimit),
		ctx:     con,
		can:     cancel,
		inchan:  in,
		outchan: make(chan T, CHANSIZE)}

	go r.mainloop()

	return &r, nil
}

func NewWithPipeline[T Sizeable](rLimit rate.Limit, bLimit int, p pipeline.Pipelineable[T]) (*RateLimiterPipe[T], error) {
	r, err := NewWithChannel(rLimit, bLimit, p.PipelineChan())
	if err != nil {
		return nil, err
	}

	r.pl = p
	return r, nil
}

func New[T Sizeable](rLimit rate.Limit, bLimit int) (*RateLimiterPipe[T], error) {
	r, err := NewWithChannel(rLimit, bLimit, make(chan T, CHANSIZE))
	if err != nil {
		return nil, err
	}

	return r, nil
}
