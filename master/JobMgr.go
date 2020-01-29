package master

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/hakits/gocrontab/common"
	"time"
)

type JobMgr struct {
	client * clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease
}

var (
	G_jobMgr *JobMgr
)

func InitJobMgr() (err error)  {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv clientv3.KV
		lease clientv3.Lease
	)

	ctxTimeOut, _ := context.WithTimeout(context.TODO(), time.Duration(G_config.EtcdDialTimeout) * time.Second)
	config = clientv3.Config{
		Endpoints:            G_config.EtcdEndpoints,
		DialTimeout:          time.Duration(G_config.EtcdDialTimeout) * time.Second,
		Context:ctxTimeOut,
	}

	//create connect
	if client, err = clientv3.New(config); err != nil {
		return
	}

	//得到kv 和lease
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)

	G_jobMgr = &JobMgr{
		client: client,
		kv:     kv,
		lease:  lease,
	}
	return
}

func (j *JobMgr)SaveJob(job *common.Job) (oldJob *common.Job, err error) {
	var (
		jobKey string
		jobValue []byte
		putResp *clientv3.PutResponse
		oldJobObj common.Job
	)
	jobKey = common.JOB_SAVE_DIR + job.Name
	if jobValue, err = json.Marshal(job); err != nil {
		return
	}

	//save to etcd
	if putResp, err = j.kv.Put(context.TODO(), jobKey, string(jobValue), clientv3.WithPrevKV()); err != nil {
		return
	}
	if putResp.PrevKv != nil {
		// 对旧值做一个反序列化
		if err = json.Unmarshal(putResp.PrevKv.Value, &oldJobObj); err != nil {
			err = nil
			return
		}
		oldJob = &oldJobObj
	}
	return
}

func (j *JobMgr) DeleteJob(name string) (oldJob *common.Job, err error)  {
	var (
		jobKey string
		delResp *clientv3.DeleteResponse
		oldJobObj common.Job
	)
	jobKey = common.JOB_SAVE_DIR + name

	if delResp, err = j.kv.Delete(context.TODO(), jobKey, clientv3.WithPrevKV());err != nil {
		return
	}

	if len(delResp.PrevKvs) != 0 {
		if err = json.Unmarshal(delResp.PrevKvs[0].Value, &oldJobObj); err != nil {
			err = nil
			return
		}
		oldJob = &oldJobObj
	}
	return 
}

//list job
func (j *JobMgr) ListJobs() (jobList []*common.Job, err error) {
	var (
		dirKey string
		getResp *clientv3.GetResponse
		kvPari *mvccpb.KeyValue
		job *common.Job
	)
	//任务保存的dir
	dirKey = common.JOB_SAVE_DIR

	//获取目录下所有的任务信息
	if getResp, err = j.kv.Get(context.TODO(), dirKey, clientv3.WithPrefix()); err != nil {
		return
	}

	//init job array
	jobList = make([]*common.Job, 0)

	for _, kvPari = range getResp.Kvs {
		job = &common.Job{}
		if err = json.Unmarshal(kvPari.Value, job); err != nil {
			fmt.Println("json unmarshal kvPari error:", err)
			err = nil
			continue
		}
		jobList = append(jobList, job)
	}
	return
}

func (j *JobMgr) KillJob(name string) (err error) {
	var (
		killerKey string
		leaseGrantResp *clientv3.LeaseGrantResponse
		leaseId clientv3.LeaseID
	)

	// 通知worker杀死对应任务
	killerKey = common.JOB_KILLER_DIR + name

	// 让worker监听到一次put操作, 创建一个租约让其稍后自动过期即可
	if leaseGrantResp, err = j.lease.Grant(context.TODO(), 1); err != nil {
		return
	}

	// 租约ID
	leaseId = leaseGrantResp.ID

	// 设置killer标记
	if _, err = j.kv.Put(context.TODO(), killerKey, "", clientv3.WithLease(leaseId)); err != nil {
		return
	}
	return

}