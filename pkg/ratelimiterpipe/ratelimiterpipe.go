package ratelimiterpipe

import (
	"context"

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
}

// InChan
func (r *RateLimiterPipe[T]) InChan() chan<- T {
	return r.inchan
}

// OutChan
func (r *RateLimiterPipe[T]) OutChan() <-chan T {
	return r.outchan
}

func (r *RateLimiterPipe[_]) SetLimit(l rate.Limit) {
	r.limit.SetLimit(l)
}

func (r *RateLimiterPipe[_]) SetBurst(n int) {
	r.limit.SetBurst(n)
}

func (r *RateLimiterPipe[_]) Close() {
	// Cancel our context
	r.can()
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

func New[T Sizeable](rLimit rate.Limit, bLimit int) (*RateLimiterPipe[T], error) {
	con, cancel := context.WithCancel(context.Background())
	r := RateLimiterPipe[T]{
		limit:   rate.NewLimiter(rLimit, bLimit),
		ctx:     con,
		can:     cancel,
		inchan:  make(chan T, CHANSIZE),
		outchan: make(chan T, CHANSIZE)}

	go r.mainloop()

	return &r, nil
}
