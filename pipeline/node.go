package pipeline

import (
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"time"
)

var startTime time.Time

func Init() {
	startTime = time.Now()
}

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
	// 给channel增加buffer, 减少block，增加效率
	out := make(chan int, 1024)
	go func() {
		a := []int{}
		// Read into memory
		for v := range in {
			a = append(a, v)
		}
		fmt.Println("Read down: ", time.Now().Sub(startTime))

		// Sort
		sort.Ints(a)
		fmt.Println("InMemSort down: ", time.Now().Sub(startTime))

		// Output
		for _, v := range a {
			out <- v
		}
		close(out)
	}()
	return out
}

func Merge(in1, in2 <-chan int) <-chan int {
	out := make(chan int, 1024)
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
		fmt.Println("Merge down: ", time.Now().Sub(startTime))
	}()

	return out
}

func MergeN(in ... <- chan int) <- chan int {
	if len(in) == 1 {
		return in[0]
	}
	m := len(in) / 2
	return Merge(MergeN(in[:m]...), MergeN(in[m:]...))
}

func ReaderSource(reader io.Reader, chunkSize int) <- chan int {
	out := make(chan int, 1024)
	go func() {
		buffer := make([]byte, 8)
		bytesRead := 0
		for {
			read, err := reader.Read(buffer)
			if read > 0 {
				out <- int(binary.BigEndian.Uint64(buffer))
				bytesRead += read
			}
			if err != nil || (chunkSize != -1 && bytesRead >= chunkSize) {
				if err != nil {
					fmt.Println("err:",err, " bytesRead:", bytesRead)
				}
				close(out)
				break
			}
		}
	}()
	return out
}

func WriterSink(writer io.Writer, in <- chan int) {
	//
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
	rand.Seed(time.Now().Unix())
	go func() {
		for i:= 0; i<count; i++ {
			out <- rand.Int()
		}
		close(out)
	}()
	return out
}
