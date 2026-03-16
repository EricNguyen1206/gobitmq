package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
)

func main() {
	fmt.Println(os.Args)

	switch os.Args[1] {
	case "server":
		startServer()
	case "client":
		clientConnect()
	default:
		fmt.Println("Usage: go run main.go [server|client]")
	}
}

func startServer() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	// Lắng nghe các kết nối TCP đến trên cổng 1234.
	ln, _ := net.Listen("tcp", ":1234")
	// Chấp nhận một kết nối mới. Hàm này sẽ tạm dừng cho đến khi có một client kết nối.
	conn, _ := ln.Accept() // Block until can
	fmt.Println(ln.Addr())

	// Tạo một đối tượng ReadWriter để đọc và ghi dữ liệu vào stream một cách hiệu quả.
	stream_rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	// Đọc byte đầu tiên từ stream, byte này chứa thông tin về độ dài của dữ liệu.
	header, _ := stream_rw.ReadByte()
	// "Nhìn trộm" (peek) vào stream để lấy dữ liệu mà không di chuyển con trỏ đọc.
	data, _ := stream_rw.Peek(int(header))

	fmt.Printf("Received from client: %s", string(data))

	// Loại bỏ dữ liệu đã đọc khỏi stream.
	stream_rw.Discard(int(header))

	// Graceful shutdown
	go func() {
		<-sigChan
		fmt.Println("Closing connection")

		err := stream_rw.Flush()
		if err != nil {
			log.Fatal(err)
		} else {
			fmt.Println("Flushed")
		}

		conn.Close()
		os.Exit(0)
	}()
}

func clientConnect() {
	// Kết nối đến server TCP đang lắng nghe trên cổng 1234.
	conn, _ := net.Dial("tcp", ":1234")
	// Đọc một dòng từ input chuẩn (bàn phím).
	rd := bufio.NewReader(os.Stdin)
	line, err := rd.ReadString('\n')
	if err != nil {
		log.Fatal(err)
	}
	// In ra màn hình nội dung sẽ gửi đến server.
	fmt.Printf("Send to server: %s", line)

	// Tạo một đối tượng ReadWriter để đọc và ghi dữ liệu vào stream một cách hiệu quả.
	stream_rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	// Ghi độ dài của dòng dữ liệu vào stream.
	stream_rw.WriteByte(byte(len(line)))
	// Ghi chính dòng dữ liệu đó vào stream.
	stream_rw.WriteString(line)
	// Đẩy tất cả dữ liệu từ bộ đệm ghi vào stream để gửi đi.
	stream_rw.Flush()

	// Đóng kết nối sau khi hoàn tất.
	conn.Close()
}
