package master

import (
	"context"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/hakits/gocrontab/common"
	"time"
)

type WorkerMgr struct {
common.EtcdClient
}

var (
	G_workerMgr *WorkerMgr
)

func (w *WorkerMgr) ListWorkers() (workers []string, err error) {
	var (
		getResp *clientv3.GetResponse
		kv *mvccpb.KeyValue
		workerIP string
	)

	workers = make([]string, 0)
	if getResp, err = w.Kv.Get(context.TODO(), common.JOB_WORKER_DIR, clientv3.WithPrefix()); err != nil {
		return
	}

	for _, kv = range getResp.Kvs {
		//kv.Key: /cron/wokers/192.168.1.10
		workerIP = common.ExtractWorkerIP(string(kv.Key))
		workers = append(workers, workerIP)
	}
	return
}

func InitWorkerMgr() (err error) {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv clientv3.KV
		lease clientv3.Lease
	)

	// 初始化配置
	config = clientv3.Config{
		Endpoints: G_config.EtcdEndpoints, // 集群地址
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Second, // 连接超时
	}

	// 建立连接
	if client, err = clientv3.New(config); err != nil {
		return
	}

	// 得到KV和Lease的API子集
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)

	G_workerMgr = &WorkerMgr{
		EtcdClient:common.EtcdClient{
			Client: client,
			Kv:     kv,
			Lease:  lease,
		},
	}
	return
}