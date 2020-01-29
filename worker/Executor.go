package worker

import (
	"fmt"
	"github.com/hakits/gocrontab/common"
	"math/rand"
	"os/exec"
	"time"
)

type Executor struct {

}

var (
	G_executor *Executor
)

func (e *Executor) ExecuteJob(info *common.JobExecuteInfo)  {
	var (
		cmd *exec.Cmd
		err error
		output []byte
		result *common.JobExecuteResult
		jobLock *JobLock
	)

	result = &common.JobExecuteResult{
		ExecuteInfo:info,
		Output:make([]byte, 0),
	}

	//初始化分布式锁
	jobLock = G_jobMgr.CreateJobLock(info.Job.Name)

	//记录开始时间
	result.StartTime = time.Now()

	//上锁
	//随机睡眠
	time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
	err = jobLock.TryLock()
	defer jobLock.Unlock()
	if err != nil {
		result.Err = err
		result.EndTime = time.Now()
		fmt.Println("请求分布式锁失败:", err)
	} else {
		result.StartTime = time.Now() //重置锁
		cmd = exec.CommandContext(info.CancelCtx, "/bin/bash", "-c", info.Job.Command)

		//执行并捕获输出
		output, err = cmd.CombinedOutput()

		//记录结束时间
		result.EndTime = time.Now()
		result.Output = output
		result.Err = err
	}
}