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
	// 这里的err == io.EOF
	if _, err = conn.Read(buffer); err != nil {
		return
	}

	// 处理命令：发回硬编码的"+PONG\r\n"
}
```

#### Handle concurrent clients

可以使用事件循环（Event Loop），但是有点复杂，之后再研究吧

使用无限循环+`listener.Accept()`来接client，之后开一个goroutine来处理之后的任务（处理命令，这里暂时是回复一个硬编码的“+PONG\r\n”）：

```go
for {
	conn, _ := l.Accept()
	
	go handleConn(conn)
}
```
ChatGPT:

`net.Conn` 是 Go 标准库中定义的一个**接口类型（interface）**，而不是具体的结构体。
接口值本身是一个很小的结构，内部包含两部分信息：

* 具体实现该接口的类型信息（例如 `*net.TCPConn`）
* 指向该具体实现对象的指针

当调用 `listener.Accept()` 时，返回的是一个 `net.Conn` 接口值，其底层通常指向同一个具体的 TCP 连接对象（`*net.TCPConn`）。

在将 `net.Conn` 作为参数传递给函数（例如 `handleConn(conn)`）时，**传递的是接口值本身的一份拷贝**，而不是复制底层的连接对象。
这份拷贝仍然指向同一个底层 TCP 连接，因此对该参数调用 `Read`、`Write`、`Close` 等方法，效果与在原始位置操作该连接完全一致。

也正因为如此，在连接处理函数中调用 `conn.Close()` 会关闭真实的网络连接，其他持有该连接的地方随后将无法再对其进行读写操作。

需要注意的是，`net.Conn` 本身已经具备引用语义，因此**不应使用 `*net.Conn`（指向接口的指针）**。
`*net.Conn` 既没有接口方法集，也无法直接调用 `Read`、`Write` 等方法，通常是错误的用法。
