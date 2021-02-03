package main

import (
	"bufio"
	"fmt"
	"goProject/pipeline"
	"os"
	"strconv"
)

func main() {
	const infilename string = "large.in"
	const outfilename string = "large.out"
	const filesize int = 800000000
	if _, err := os.Stat(infilename); err != nil && !os.IsExist(err) {
		createFile(filesize, infilename)
	}
	// p := createPipeline(infilename, filesize, 8)
	p := createNetworkPipeline(infilename, filesize, 8)
	writeToFile(p, outfilename)		// createPipeline不干活，writeToFile才开始从pipleline里读取数据
	printFile(outfilename)
}


func createFile(filesize int, filename string) {
	randSource := pipeline.RandomSource(filesize / 8)
	file, err := os.Create(filename)
	if err != nil {
		panic(nil)
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	pipeline.WriterSink(writer, randSource)
	writer.Flush()
}

func printFile(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	p := pipeline.ReaderSource(file, 512)
	for num := range p {
		fmt.Println(num)
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
	注意：函数返回的pipeline, 返回时，打开的文件还没有读取结束，不能close
	     需要将文件描述符返回给调用者来close
	 */
	chunkSize := fileSize / chunkCount
	mergeInputs := []<-chan int{}
	pipeline.Init()
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

func createNetworkPipeline(filename string, fileSize, chunkCount int) <-chan int {
	/*
		注意：函数返回的pipeline, 返回时，打开的文件还没有读取结束，不能close
		     需要将文件描述符返回给调用者来close
	*/
	chunkSize := fileSize / chunkCount
	sortAddr := []string {}
	pipeline.Init()
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
		addr := ":" + strconv.Itoa(7000 + i)
		pipeline.NetworkSink(addr, pipeline.InMemSort(input))
		sortAddr = append(sortAddr, addr)
	}
	mergeInputs := []<-chan int{}
	for _, addr := range sortAddr {
		mergeInputs = append(mergeInputs, pipeline.NetworkSource(addr))
	}
	return pipeline.MergeN(mergeInputs...)
}
