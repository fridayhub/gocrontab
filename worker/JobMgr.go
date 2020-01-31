package worker

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/hakits/gocrontab/common"
	"time"
)

type JobMgr struct {
	common.EtcdClient
	watcher clientv3.Watcher
}

var (
	GJobmgr *JobMgr
)

func (j *JobMgr) watchKiller() {
	var (
		watchChan  clientv3.WatchChan
		watchResp  clientv3.WatchResponse
		watchEvent *clientv3.Event
		jobEvent   *common.JobEvent
		jobName    string
		job        *common.Job
	)
	// 监听/cron/killer目录
	go func() { // 监听协程
		// 监听/cron/killer/目录的变化
		watchChan = j.watcher.Watch(context.TODO(), common.JOB_KILLER_DIR, clientv3.WithPrefix())
		// 处理监听事件
		for watchResp = range watchChan {
			for _, watchEvent = range watchResp.Events {
				switch watchEvent.Type {
				case mvccpb.PUT: // 杀死任务事件
					jobName = common.ExtractKillerName(string(watchEvent.Kv.Key))
					job = &common.Job{Name: jobName}
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_KILL, job)
					// 事件推给scheduler
					GScheduler.PushJobEvent(jobEvent)
				case mvccpb.DELETE: // killer标记过期, 被自动删除
				}
			}
		}
	}()

}

func (j *JobMgr) watchJobs() (err error) {
	fmt.Println("watchJobs start ...")
	var (
		getResp            *clientv3.GetResponse
		kvpair             *mvccpb.KeyValue
		job                *common.Job
		watchStartRevision int64
		watchChan          clientv3.WatchChan
		watchResp          clientv3.WatchResponse
		watchEvent         *clientv3.Event
		jobName            string
		jobEvent           *common.JobEvent
	)

	// 1, get一下/cron/jobs/目录下的所有任务，并且获知当前集群的revision
	if getResp, err = j.Kv.Get(context.TODO(), common.JOB_SAVE_DIR, clientv3.WithPrefix()); err != nil {
		return
	}
	//遍历任务
	for _, kvpair = range getResp.Kvs {
		if job, err = common.UnpackJob(kvpair.Value); err != nil {
			fmt.Println("unpackJob error:", err)
		}
		jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE, job)
		GScheduler.PushJobEvent(jobEvent)
	}

	//从该revision向后监听变化事件
	go func() {
		watchStartRevision = getResp.Header.Revision + 1
		watchChan = j.watcher.Watch(context.TODO(), common.JOB_SAVE_DIR, clientv3.WithRev(watchStartRevision), clientv3.WithPrefix())
		for watchResp = range watchChan {
			for _, watchEvent = range watchResp.Events {
				switch watchEvent.Type {
				case mvccpb.PUT: //save job event
					if job, err = common.UnpackJob(watchEvent.Kv.Value); err != nil {
						fmt.Println("unpackJob error:", err)
					}
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE, job)
				case mvccpb.DELETE:
					jobName = common.ExtractJobName(string(watchEvent.Kv.Key))
					job = &common.Job{Name: jobName}
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_DELETE, job)
				}
				GScheduler.PushJobEvent(jobEvent)
			}
		}
	}()
	return
}

// 初始化管理器
func InitJobMgr() (err error) {
	var (
		config  clientv3.Config
		client  *clientv3.Client
		kv      clientv3.KV
		lease   clientv3.Lease
		watcher clientv3.Watcher
	)

	// 初始化配置
	config = clientv3.Config{
		Endpoints:   GConfig.EtcdEndpoints,                                // 集群地址
		DialTimeout: time.Duration(GConfig.EtcdDialTimeout) * time.Second, // 连接超时
	}

	// 建立连接
	if client, err = clientv3.New(config); err != nil {
		return
	}

	// 得到KV和Lease的API子集
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)
	watcher = clientv3.NewWatcher(client)

	// 赋值单例
	GJobmgr = &JobMgr{
		EtcdClient: common.EtcdClient{
			Client: client,
			Kv:     kv,
			Lease:  lease,
		},
		watcher: watcher,
	}

	// 启动任务监听
	GJobmgr.watchJobs()

	// 启动监听killer
	GJobmgr.watchKiller()

	return
}

func (j *JobMgr) CreateJobLock(jobName string) *JobLock {
	return InitJobLock(jobName, j.Kv, j.Lease)
}
