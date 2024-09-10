package main

import (
	"bufio"
	"fmt"
	"net"
	"time"
)

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:16379")
	if err != nil {
		fmt.Println("连接失败:", err)
		return
	}
	defer conn.Close()

	// // 发送 PING 命令
	// fmt.Fprintf(conn, "*1\r\n$4\r\nPING\r\n")

	// // 读取并打印响应
	// reader := bufio.NewReader(conn)
	// response, err := reader.ReadString('\n')
	// if err != nil {
	// 	fmt.Println("读取响应失败:", err)
	// 	return
	// }

	// fmt.Println("响应:", response)

	// 测试 SET 命令
	// testCommand(conn, "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")                                       // 设置 key-value
	// testCommand(conn, "*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$7\r\nnew_val\r\n$2\r\nNX\r\n$2\r\nEX\r\n$2\r\n10\r\n") // 带有 NX EX 选项

	// // 测试 SETNX 命令
	// testCommand(conn, "*3\r\n$5\r\nSETNX\r\n$4\r\nkey2\r\n$6\r\nvalue2\r\n") // 设置 key2-value2

	// // 测试 SETEX 命令
	// testCommand(conn, "*4\r\n$5\r\nSETEX\r\n$4\r\nkey3\r\n$2\r\n10\r\n$6\r\nvalue3\r\n") // 设置 key3-value3，过期时间 10 秒

	// // 测试 PSETEX 命令
	// testCommand(conn, "*4\r\n$6\r\nPSetEX\r\n$4\r\nkey4\r\n$3\r\n500\r\n$6\r\nvalue4\r\n") // 设置 key4-value4，过期时间 500 毫秒

	testCommand(conn, "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n") // 设置 key-value

	// 测试 GET 命令
	testCommand(conn, "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n") // 获取 key 的值

	// 测试不存在的 key
	testCommand(conn, "*2\r\n$3\r\nGET\r\n$7\r\nno_key\r\n") // 获取 no_key 的值

	time.Sleep(time.Second)
}

func testCommand(conn net.Conn, command string) {
	// fmt.Println("发送命令:", command)
	fmt.Fprint(conn, command)

	reader := bufio.NewReader(conn)
	response, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("读取响应失败:", err)
		return
	}

	fmt.Println("响应:", response)
}
