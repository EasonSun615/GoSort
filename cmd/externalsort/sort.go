package main

import (
	"bufio"
	"fmt"
	"goProject/pipeline"
	"os"
)

func main() {
	const infilename string = "small.in"
	const outfilename string = "small.out"
	const filesize int = 64
	randSource := pipeline.RandomSource(filesize / 8)
	file, err := os.Create(infilename)
	if err != nil {
		panic(nil)
	}
	defer file.Close()
	pipeline.WriterSink(file, randSource)
	printFile(infilename)
	fmt.Println("####################")
	p := createPipeline(infilename,filesize, 4)
	writeToFile(p, outfilename)
	printFile(outfilename)
}

func printFile(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	p := pipeline.ReaderSource(file, -1)
	count := 0
	for num := range p {
		fmt.Println(num)
		count ++
		if count > 100 {
			break
		}
	}
}

func writeToFile(p <-chan int, filename string) {
	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	pipeline.WriterSink(writer, p)
	writer.Flush()
}


func createPipeline(filename string, fileSize, chunkCount int) <-chan int {
	/*
	注意：函数返回的时pipeline, 返回时，打开的文件还没有读取结束，不能close
	     需要将文件描述符返回给调用者来close
	 */
	chunkSize := fileSize / chunkCount
	mergeInputs := []<-chan int{}
	for i := 0; i < chunkCount; i++ {
		file, err := os.Open(filename)
		if err != nil {
			panic(nil)
		}
		_, err = file.Seek(int64(i*chunkSize), 0)
		if err != nil {
			panic(err)
		}
		input := pipeline.ReaderSource(bufio.NewReader(file), chunkSize)
		mergeInputs = append(mergeInputs, pipeline.InMemSort(input))
	}
	return pipeline.MergeN(mergeInputs...)
}