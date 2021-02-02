package main

import (
	"bufio"
	"fmt"
	"goProject/pipeline"
	"os"
)

func ArraySourceDemo() {
	src := pipeline.ArraySource(3, 45, 1, 4, 5, 5, 31, 5)
	// for v := range src {
	// 	fmt.Println(v)
	// }
	for {
		if c, ok := <-src; ok {
			fmt.Println(c)
		} else {
			break
		}
	}
}

func InMemSortDemo() {
	p := pipeline.InMemSort(
		pipeline.ArraySource(4, 1, 6, 12, 7, 2, 9, 41, 6))
	// GoLang不需要锁，如果out里的数据没有准备好，会自动阻塞
	for v := range p {
		fmt.Println(v)
	}
}

func MergeDemo() {
	p := pipeline.Merge(
		pipeline.InMemSort(
			pipeline.ArraySource(4, 1, 6, 12, 7, 2, 9, 41, 6)),
		pipeline.InMemSort(
			pipeline.ArraySource(324,45,1,3,4,5123,7,5)))
	// GoLang不需要锁，如果out里的数据没有准备好，会自动阻塞
	for v := range p {
		fmt.Println(v)
	}
}

func main(){
	const filename = "small.in"
	const count = 64
	f, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	source := pipeline.RandomSource(count)
	writer := bufio.NewWriter(f)
	pipeline.WriterSink(writer, source)
	writer.Flush()
	f, err = os.Open("small.in")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	n := 0
	p := pipeline.ReaderSource(bufio.NewReader(f), -1)
	for num := range p {
		fmt.Println(num)
		n++
		if n> 20{
			break
		}
	}
}
