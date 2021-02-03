package pipeline

import (
	"bufio"
	"net"
)

/*
开启一个server，和client建立连接之后，将channel中的所有数据倒给client
 */
func NetworkSink(addr string, in <- chan int) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	// 节点在pipeline中，所以节点不可以干活, 需要开个协程去干活
	go func() {
		defer listener.Close()
		// 只接收一次连接，不用循环accept
		conn, err := listener.Accept()
		if err != nil {
			panic(err)
		}
		defer conn.Close()

		// 使用bufio包装一下conn
		writer := bufio.NewWriter(conn)
		defer writer.Flush()
		WriterSink(writer, in)
	}()
}


func NetworkSourceLow(addr string) (<- chan int, net.Conn) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		panic(err)
	}
	// todo conn.Close()
	return ReaderSource(bufio.NewReader(conn), -1), conn
}

func NetworkSource(addr string) <- chan int {
	out := make(chan int)
	go func() {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			panic(err)
		}
		defer conn.Close()
		r := ReaderSource(bufio.NewReader(conn), -1)
		for num := range r{
			out <- num
		}
		close(out)
	}()
	return out
}