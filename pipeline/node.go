package pipeline

import (
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"sort"
)

func ArraySource(a ...int) <-chan int {
	out := make(chan int)
	go func() {
		for _, v := range a {
			out <- v
		}
		close(out)
	}()
	return out
}

func InMemSort(in <-chan int) <-chan int {
	out := make(chan int)
	go func() {
		a := []int{}
		// Read into memory
		for v := range in {
			a = append(a, v)
		}
		// Sort
		sort.Ints(a)

		// Output
		for _, v := range a {
			out <- v
		}
		close(out)
	}()
	return out
}

func Merge(in1, in2 <-chan int) <-chan int {
	out := make(chan int)
	go func() {
		num1, ok1 := <-in1
		num2, ok2 := <-in2
		for ok1 || ok2 {
			if !ok2 || (ok1 && num1 < num2) {
				out <- num1
				num1, ok1 = <-in1
			} else {
				out <- num2
				num2, ok2 = <-in2
			}
		}
		close(out)
	}()

	return out
}

func ReaderSource(reader io.Reader) <- chan int {
	out := make(chan int)
	go func() {
		buffer := make([]byte, 8)
		for {
			read, err := reader.Read(buffer)
			if read > 0 {
				out <- int(binary.BigEndian.Uint64(buffer))
			}
			if err != nil {
				close(out)
				break
			}
		}
	}()
	return out
}

func WriterSink(writer io.Writer, in <- chan int) {
	for num := range in {
		buffer := make([]byte, 8)
		binary.BigEndian.PutUint64(buffer, uint64(num))
		if _, err := writer.Write(buffer); err != nil {
			fmt.Println("Sink Error")
		}
	}
}

func RandomSource(count int) <- chan int {
	out := make(chan int)
	go func() {
		for i:= 0; i<count; i++ {
			out <- rand.Intn(200)
		}
		close(out)
	}()
	return out
}
