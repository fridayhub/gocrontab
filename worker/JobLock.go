package worker

import (
	"context"
	"github.com/coreos/etcd/clientv3"
	"github.com/hakits/gocrontab/common"
)

type JobLock struct {
	kv clientv3.KV
	lease clientv3.Lease
	jobName string
	cancelFunc context.CancelFunc
	leaseId clientv3.LeaseID
	isLocked bool
}

func InitJobLock(jobName string, kv clientv3.KV, lease clientv3.Lease) *JobLock {
	return  &JobLock{
		kv:         kv,
		lease:      lease,
		jobName:    jobName,
	}
}

func (j *JobLock)TryLock() (err error) {
	var (
		leaseGrantResp *clientv3.LeaseGrantResponse
		cancelCtx context.Context
		cancelFunc context.CancelFunc
		leaseId clientv3.LeaseID
		keepRespChan <- chan *clientv3.LeaseKeepAliveResponse
		txn clientv3.Txn
		lockKey string
		txnResp *clientv3.TxnResponse
	)

	//1 创建租约
	if leaseGrantResp, err = j.lease.Grant(context.TODO(), 5); err != nil {
		return
	}
	// context 用于取消自动续租
	cancelCtx, cancelFunc = context.WithCancel(context.TODO())

	leaseId = leaseGrantResp.ID

	//2 自动续租
	if keepRespChan, err = j.lease.KeepAlive(cancelCtx, leaseId); err != nil {
		goto FAIL
	}

	// 3 处理续租应答的协成
	go func() {
		var (
			keepResp * clientv3.LeaseKeepAliveResponse
		)
		for {
			select {
			case keepResp = <-keepRespChan:
				if keepResp == nil {
					goto END
				}
			}
		}
		END:
	}()

	//4 创建事物txn
	txn = j.kv.Txn(context.TODO())

	//锁路径
	lockKey = common.JOB_LOCK_DIR + j.jobName
	//5 失误抢锁
	txn.If(clientv3.Compare(clientv3.CreateRevision(lockKey), "=", 0)).
		Then(clientv3.OpPut(lockKey, "", clientv3.WithLease(leaseId))).
		Else(clientv3.OpGet(lockKey))
	//commit
	if txnResp, err = txn.Commit();err != nil {
		goto FAIL
	}
	//6 成功返回，失败释放租约
	if !txnResp.Succeeded {
		err = common.ERR_LOCK_ALREADY_REQUIRED
		goto FAIL
	}

	// 抢锁成功
	j.leaseId = leaseId
	j.cancelFunc = cancelFunc
	j.isLocked = true

	FAIL:
		cancelFunc()
		j.lease.Revoke(context.TODO(), leaseId)
	return
}

// 释放锁
func (j *JobLock) Unlock() {
	if j.isLocked {
		j.cancelFunc()                            // 取消我们程序自动续租的协程
		j.lease.Revoke(context.TODO(), j.leaseId) // 释放租约
	}
}