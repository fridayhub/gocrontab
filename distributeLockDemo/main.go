package main

import (
	"context"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	"time"
)

func main() {
	//lease 实现锁自动过期
	//op 操作
	//txn事物: if else then
	var (
		config         clientv3.Config
		client         *clientv3.Client
		err            error
		lease          clientv3.Lease
		leaseGrantResp *clientv3.LeaseGrantResponse
		leaseId        clientv3.LeaseID
		keepRespChan   <-chan *clientv3.LeaseKeepAliveResponse
		//putResp        *clientv3.PutResponse
		ctx            context.Context
		cancelFunc     context.CancelFunc
		txn            clientv3.Txn
		txnRep         *clientv3.TxnResponse
		job            string
		kv             clientv3.KV
	)
	job = "/cron/lock/job1"

	ctxTimeOut, _ := context.WithTimeout(context.TODO(), 1)
	config = clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 1 * time.Second,
		Context:ctxTimeOut,
	}

	//connect to server
	if client, err = clientv3.New(config); err != nil {
		fmt.Println("connect error:", err)
		return
	}

	//1.上锁（创建租约，自动续租，拿着租约抢占一个key）
	//申请一个lease
	lease = clientv3.NewLease(client)

	//申请一个10秒租约
	leaseGrantResp, err = lease.Grant(context.TODO(), 10)
	if err != nil {
		fmt.Println("lease grant error:", err)
		return
	}
	//拿到leaseId
	leaseId = leaseGrantResp.ID

	//准备一个用于取消自动续租的context
	ctx, cancelFunc = context.WithCancel(context.TODO())
	defer cancelFunc()
	defer lease.Revoke(ctx, leaseId)
	// 自动续租
	keepRespChan, err = lease.KeepAlive(ctx, leaseId)
	if err != nil {
		fmt.Println("lease KeepAlive error:", err)
		return
	}
	//kv = clientv3.NewKV(client)
	//
	////put 一个kv，让他与租约关联起来，从而实现10秒后自动过期
	//putResp, err = kv.Put(context.TODO(), job, "", clientv3.WithLease(leaseId))
	//if err != nil {
	//	fmt.Println("kv put error:", err)
	//	return
	//}
	//fmt.Println("Write successful:", putResp.Header.Revision)
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

	//抢事物 if 不存在key，then设置它，else抢锁失败
	kv = clientv3.NewKV(client)

	//创建事物
	txn = kv.Txn(context.TODO())
	//定义事物
	txn.If(clientv3.Compare(clientv3.CreateRevision(job), "=", 0)).
		Then(clientv3.OpPut(job, "v111", clientv3.WithLease(leaseId))).
		Else(clientv3.OpGet(job)) //抢锁失败
	txnRep, err = txn.Commit()
	if err != nil {
		fmt.Println("txn commit error:", err)
		return
	}

	//判断是否抢占成功
	if txnRep.Succeeded {
		fmt.Println("抢占成功")
	} else {
		fmt.Println("抢占失败，锁占用者:", string(txnRep.Responses[0].GetResponseRange().Kvs[0].Value))
		return
	}
	//2.处理业务
	fmt.Println("开始执行业务逻辑。。。。")
	time.Sleep(10 * time.Second)
	//3.释放锁(取消自动续租，释放租约)
	//defer 会把租约释放
}
