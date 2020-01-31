package worker

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/hakits/gocrontab/common"
	"net"
	"time"
)

type Register struct {
	common.EtcdClient
	localIp string
}

var (
	GRegister *Register
)

func getLocalIp() (ipv4 string, err error)  {
	var (
		addrs []net.Addr
		addr net.Addr
		ipNet *net.IPNet
		isIpNet bool
		netInterfaces []net.Interface
	)
	netInterfaces, err = net.Interfaces()
	if err != nil {
		fmt.Println("net.Interfaces error:", err)
		return
	}

	for i := 0; i < len(netInterfaces); i++ {
		if (netInterfaces[i].Flags & net.FlagUp) != 0{
			if addrs, err = net.InterfaceAddrs(); err != nil {
				return
			}
			for _, addr = range addrs {
				if ipNet, isIpNet = addr.(*net.IPNet); isIpNet && !ipNet.IP.IsLoopback() {
					if ipNet.IP.To4() != nil {
						ipv4 = ipNet.IP.String()
						return
					}
				}
			}
		}
	}
	err = common.ERR_NO_LOCAL_IP_FOUND
	return
}

//注册到 /cron/workers/IP, 并自动续租
func (r *Register) keepOnline()  {
	var (
		regKey string
		leaseGrantResp *clientv3.LeaseGrantResponse
		err error
		keepAliveChan <- chan *clientv3.LeaseKeepAliveResponse
		keepAliveResp *clientv3.LeaseKeepAliveResponse
		cancelCtx context.Context
		cancelFunc context.CancelFunc
	)
	for {
		regKey = common.JOB_WORKER_DIR + r.localIp
		cancelFunc = nil

		//create lease
		if leaseGrantResp, err = r.Lease.Grant(context.TODO(), 10); err != nil {
			goto RETRY
		}

		//自动续租
		if keepAliveChan, err = r.Lease.KeepAlive(context.TODO(), leaseGrantResp.ID); err != nil {
			goto RETRY
		}

		cancelCtx, cancelFunc = context.WithCancel(context.TODO())

		if _, err = r.Kv.Put(cancelCtx, regKey, "", clientv3.WithLease(leaseGrantResp.ID)); err != nil {
			goto RETRY
		}

		for {
			select {
			case keepAliveResp = <- keepAliveChan:
				if keepAliveResp == nil {
					goto RETRY
				}
			}
		}

		RETRY:
			time.Sleep(1 * time.Second)
		if cancelFunc != nil {
			cancelFunc()
		}
	}
}

func InitRegister() (err error)  {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv clientv3.KV
		lease clientv3.Lease
		localIp string
	)
	config = clientv3.Config{
		Endpoints:   GConfig.EtcdEndpoints,
		DialTimeout: time.Duration(GConfig.EtcdDialTimeout) * time.Second,
	}

	//connect to server
	if client, err = clientv3.New(config); err != nil {
		return
	}

	if localIp, err = getLocalIp(); err != nil {
		return
	}

	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)
	GRegister = &Register{
		EtcdClient: common.EtcdClient{
			Client: client,
			Kv:     kv,
			Lease:  lease,
		},
		localIp:    localIp,
	}

	go GRegister.keepOnline()
	return
}
