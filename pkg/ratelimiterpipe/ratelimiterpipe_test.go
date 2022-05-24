package ratelimiterpipe_test

import (
	"fmt"

	"github.com/sterlingdevils/ratelimiterpipe/pkg/ratelimiterpipe"
)

type DataType string

type Node struct {
	data DataType
}

func (n Node) Size() int {
	return len(n.data)
}

func ExampleNew() {
	_, _ = ratelimiterpipe.New[Node](1, 2)
	// Output:
	//
}

func Example_testsend() {
	n := Node{data: "potatoes"}
	r, _ := ratelimiterpipe.New[Node](4, n.Size())
	r.InChan() <- n
	t := <-r.OutChan()
	fmt.Println(t.data)
	// Output:
	// potatoes
}

func Example_testsend2() {
	n := Node{data: "potatoes"}
	r, _ := ratelimiterpipe.New[Node](1, n.Size())
	r.InChan() <- n
	t := <-r.OutChan()
	r.InChan() <- n
	t = <-r.OutChan()
	fmt.Println(t.data)
	// Output:
	// potatoes
}
