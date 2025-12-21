## Redis-Go Challenge

### Intro

#### Respond to multiple PINGs

在无限循环中读取client发来的命令并发回respond。

一开始没看懂要求，后来发现意思实际上是在同一个connection里处理多个request：

```go
conn, _ := l.Accept()
buffer := make([]byte, 1024) // 用于接收Read到的命令

for {
	// 没读到命令：退出循环
	// 用io.EOF也行？
	if _, err = conn.Read(buffer); err != nil {
		return
    }

	// 处理命令：发回硬编码的"+PONG\r\n"
}
```