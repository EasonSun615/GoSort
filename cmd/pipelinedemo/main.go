package main

import (
	"fmt"
	"goProject/pipeline"
)


func main1() {
	src := pipeline.ArraySource(3,45,1,4,5,5,31,5)
	// for v := range src {
	// 	fmt.Println(v)
	// }
	for {
		if c, ok := <-src; ok {
			fmt.Println(c)
		}else{
			break
		}
	}
}

func main(){
	p := pipeline.InMemSort(
		pipeline.ArraySource(4,1,6,12,7,2,9,41,6))
	// GoLang不需要锁，如果out里的数据没有准备好，会自动阻塞
	for v := range p{
		fmt.Println(v)
	}
}
