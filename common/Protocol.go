package common

import (
	"context"
	"encoding/json"
	"github.com/gorhill/cronexpr"
	"strings"
	"time"
)

type Job struct {
	Name string `json:"name"`
	Command string `json:"command"`
	CronExpr string `json:"cronExpr"`
}

// 任务调度计划
type JobSchedulePlan struct {
	Job *Job
	Expr * cronexpr.Expression
	NextTime time.Time
}

//任务执行状态
type JobExecuteInfo struct {
	Job *Job //任务信息
	PlanTime time.Time //理论调度时间
	RealTime time.Time //实际调度时间
	CancelCtx context.Context  // 任务command的context
	CancelFunc context.CancelFunc // 用于取消command执行的cancel函数
}

//http接口应答
type Response struct {
	Errno int `json:"errno"`
	Msg string `json:"msg"`
	Data interface{} `json:"data"`
}

// 变化事件
type JobEvent struct {
	EventType int // SAVE, DELETE
	Job *Job
}

//任务执行结果
type JobExecuteResult struct {
	ExecuteInfo * JobExecuteInfo //执行状态
	Output []byte
	Err error
	StartTime time.Time //启动时间
	EndTime time.Time //结束时间
}

// 任务执行日志
type JobLog struct {
	JobName string `json:"jobName"` // 任务名字
	Command string `json:"command"` // 脚本命令
	Err string `json:"err"` // 错误原因
	Output string `json:"output"`	// 脚本输出
	PlanTime int64 `json:"planTime"` // 计划开始时间
	ScheduleTime int64 `json:"scheduleTime"` // 实际调度时间
	StartTime int64 `json:"startTime"` // 任务执行开始时间
	EndTime int64 `json:"endTime"` // 任务执行结束时间
}

//update job or del job
func BuildJobEvent(eventType int, job *Job) (jobEvent *JobEvent) {
	return &JobEvent{
		EventType: eventType,
		Job:       job,
	}
}

// build http response msg
func BuildResponse(errno int, msg string, data interface{})(resp []byte, err error)  {
	var (
		response Response
	)
	response.Errno = errno
	response.Msg = msg
	response.Data = data

	resp, err = json.Marshal(response)
	return
}

// 反序列化Job
func UnpackJob(value []byte) (ret *Job, err error) {
	var (
		job *Job
	)

	job = &Job{}
	if err = json.Unmarshal(value, job); err != nil {
		return
	}
	ret = job
	return
}

func ExtractWorkerIP(regKey string) string  {
	return strings.TrimPrefix(regKey, JOB_WORKER_DIR)
}