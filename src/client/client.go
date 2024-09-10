package main

import (
	"bufio"
	"fmt"
	"net"
	"time"
)

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:16399")
	if err != nil {
		fmt.Println("连接失败:", err)
		return
	}
	defer conn.Close()

	// 发送 PING 命令
	fmt.Fprintf(conn, "*1\r\n$4\r\nPING\r\n")

	// 读取并打印响应
	reader := bufio.NewReader(conn)
	response, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("读取响应失败:", err)
		return
	}

	fmt.Println("响应:", response)

	time.Sleep(time.Second)
}
