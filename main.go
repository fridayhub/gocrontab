package main

import (
	"context"
	"fmt"
	"github.com/gorhill/cronexpr"
	"go.etcd.io/etcd/clientv3"
	"os/exec"
	"time"
)

func cronDemo1() {
	expr, err := cronexpr.Parse("* * * * *")
	if err != nil {
		fmt.Println("parse error:", err)
	}
	nextTime := expr.Next(time.Now())
	fmt.Println(time.Now(), nextTime)
}

func etcd() {
	var (
		config clientv3.Config
		client *clientv3.Client
		err    error
	)

	ctxTimeOut, _ := context.WithTimeout(context.TODO(), 1)
	config = clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
		Context:ctxTimeOut,
	}

	//connect to server
	if client, err = clientv3.New(config); err != nil {
		fmt.Println("connect error:", err)
		return
	}

	kv := clientv3.NewKV(client)
	putResp, err := kv.Put(context.TODO(), "/cron/jobs/jobs1", "hello")
	if err != nil {
		fmt.Println("put error:", err)
		return
	}
	fmt.Println("Revision:", putResp.Header.Revision)

	getResp, err := kv.Get(context.TODO(), "/cron/jobs/jobs1")
	if err != nil {
		fmt.Println("get error:", err)
		return
	}
	fmt.Println("get:", getResp.Kvs)

	getResp, err = kv.Get(context.TODO(), "/cron/jobs/", clientv3.WithPrefix())
	if err != nil {
		fmt.Println("get error:", err)
		return
	}
	fmt.Println("prefix get:", getResp.Kvs)
}

func etcdLease() {
	var (
		config clientv3.Config
		client *clientv3.Client
		err    error
	)

	ctxTimeOut, _ := context.WithTimeout(context.TODO(), 1)
	config = clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
		Context:ctxTimeOut,
	}

	//connect to server
	if client, err = clientv3.New(config); err != nil {
		fmt.Println("connect error:", err)
		return
	}

	//申请一个lease
	lease := clientv3.NewLease(client)

	//申请一个10秒租约
	leaseGrantResp, err := lease.Grant(context.TODO(), 10)
	if err != nil {
		fmt.Println("lease grant error:", err)
		return
	}
	//拿到leaseId
	leaseId := leaseGrantResp.ID

	// 自动续租
	keepRespChan, err := lease.KeepAlive(context.TODO(), leaseId)
	if err != nil {
		fmt.Println("lease KeepAlive error:", err)
		return
	}
	kv := clientv3.NewKV(client)

	//put 一个kv，让他与租约关联起来，从而实现10秒后自动过期
	putResp, err := kv.Put(context.TODO(), "/cron/lock/job1", "", clientv3.WithLease(leaseId))
	if err != nil {
		fmt.Println("kv put error:", err)
		return
	}
	fmt.Println("Write successful:", putResp.Header.Revision)
	go func() {
		for {
			select {
			case keepResp := <-keepRespChan:
				if keepRespChan == nil {
					fmt.Println("lease invalided.")
					goto END
				} else {
					fmt.Println("recv lease response:", keepResp.ID)
				}
			}
		}
	END:
	}()
}

func main() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
	)
	ctx, cancel = context.WithCancel(context.TODO())
	go func() {
		cmd := exec.CommandContext(ctx, "/bin/bash", "-c", "sleep 2; echo hello")
		out, err := cmd.CombinedOutput()
		if err != nil {
			fmt.Println("err:", err)
		}
		fmt.Println("out put:", string(out))
	}()
	time.Sleep(3 * time.Second)
	cancel()
	cronDemo1()
	etcd()

		etcdLease()
	for {
		;
	}
}
