package main
import (
    "net"
    "os"
	"sync"
	"sync/atomic"
	"time"
	"fmt"
	//"math/rand"
)

func main() {
	var total_req_per_second int64 
	var data int64
	total_req_per_second=0
	data=0
	//
	total_client:=5000
	//
	var wg sync.WaitGroup
	write_data:= make([]byte, 1024*8)
    servAddr := "localhost:8888"
    tcpAddr, err := net.ResolveTCPAddr("tcp", servAddr)
    if err != nil {
        println("ResolveTCPAddr failed:", err.Error())
        os.Exit(1)
    }
	//
	var conns []*net.TCPConn
	conns = make([]*net.TCPConn, total_client)
	//
	real_total_client:=0
	for i:=0; i<total_client; i++{
		if conns[i]==nil{
			var err error 
			conns[i], err = net.DialTCP("tcp", nil, tcpAddr)
			if err != nil {
				println("Dial failed:", err.Error())
				//os.Exit(1)
			}else{
				real_total_client+=1
			}
		}
	}
	fmt.Println("Connected to server: ", real_total_client)
	go func() {
		for {
			for i:=0; i<real_total_client; i++{
				wg.Add(1)
				go func(i int){
					defer wg.Done()
					
					for j:=1;j<=100;j++{
						//go func(i int){
							_, err = conns[i].Write([]byte(write_data))
							if err != nil {
								println("Write to server failed:", err.Error())
								//os.Exit(1)
								continue
							}
							reply := make([]byte, 1024*8)
							_, err = conns[i].Read(reply)
							if err != nil {
								println("Write to server failed:", err.Error())
								//os.Exit(1)
								continue
							}
							atomic.AddInt64(&total_req_per_second, 1)
							atomic.AddInt64(&data, 1024*8)//bye
							//elapsed := end.Sub(start)
							//fmt.Printf("time=%v\n", elapsed.Milliseconds())
							//println("reply from server=", string(reply))
						//}(i)
					}			
				}(i)
			}
			wg.Wait()
		}
	}()
	//show result
	for {
		time.Sleep(time.Second)
		fmt.Printf("Requests/s: %v ; AVG bandwidth: %v (mb/s); \n", atomic.SwapInt64(&total_req_per_second, 0), (atomic.SwapInt64(&data, 0)/1024)/1024)
	}
	    
}