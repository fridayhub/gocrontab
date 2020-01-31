package worker

import (
	"fmt"
	"github.com/hakits/gocrontab/common"
	"time"
)

type Scheduler struct {
	jobEventChan chan *common.JobEvent
	jobPlanTable map[string]*common.JobSchedulePlan
	jobExecutingTable map[string]*common.JobExecuteInfo
	jobResultChan chan *common.JobExecuteResult
}

var (
	GScheduler *Scheduler
)

// 处理任务事件
func (s *Scheduler) handleJobEvent(jobEvent *common.JobEvent) {
	fmt.Println("handleJobEvent start ...")
	var (
		jobSchedulePlan *common.JobSchedulePlan
		jobExecuteInfo *common.JobExecuteInfo
		jobExecuting bool
		jobExisted bool
		err error
	)
	switch jobEvent.EventType {
	case common.JOB_EVENT_SAVE:	// 保存任务事件
		if jobSchedulePlan, err = common.BuildJobSchedulePlan(jobEvent.Job); err != nil {
			return
		}
		s.jobPlanTable[jobEvent.Job.Name] = jobSchedulePlan
	case common.JOB_EVENT_DELETE: // 删除任务事件
		if jobSchedulePlan, jobExisted = s.jobPlanTable[jobEvent.Job.Name]; jobExisted {
			delete(s.jobPlanTable, jobEvent.Job.Name)
		}
	case common.JOB_EVENT_KILL: // 强杀任务事件
		// 取消掉Command执行, 判断任务是否在执行中
		if jobExecuteInfo, jobExecuting = s.jobExecutingTable[jobEvent.Job.Name]; jobExecuting {
			jobExecuteInfo.CancelFunc()	// 触发command杀死shell子进程, 任务得到退出
		}
	}
}


// 尝试执行任务
func (s *Scheduler) TryStartJob(jobPlan *common.JobSchedulePlan) {
	//fmt.Println("TryStartJob start...")
	// 调度 和 执行 是2件事情
	var (
		jobExecuteInfo *common.JobExecuteInfo
		jobExecuting bool
	)

	// 执行的任务可能运行很久, 1分钟会调度60次，但是只能执行1次, 防止并发！

	// 如果任务正在执行，跳过本次调度
	if jobExecuteInfo, jobExecuting = s.jobExecutingTable[jobPlan.Job.Name]; jobExecuting {
		//fmt.Println("尚未退出,跳过执行:", jobPlan.Job.Name)
		return
	}

	// 构建执行状态信息
	jobExecuteInfo = common.BuildJobExecuteInfo(jobPlan)

	// 保存执行状态
	s.jobExecutingTable[jobPlan.Job.Name] = jobExecuteInfo

	// 执行任务
	fmt.Println("执行任务:", jobExecuteInfo.Job.Name, jobExecuteInfo.PlanTime, jobExecuteInfo.RealTime)
	go GExecutor.ExecuteJob(jobExecuteInfo)
}


//重新计算任务调度状态
func (s *Scheduler) TrySchedule() (scheduleAfter time.Duration)  {
	//fmt.Println("TrySchedule start...")
	var (
		jobPlan *common.JobSchedulePlan
		now time.Time
		nearTime *time.Time
	)

	if len(s.jobPlanTable) == 0{
		scheduleAfter = 1 * time.Second
		return
	}

	now = time.Now()
	for _, jobPlan = range s.jobPlanTable {
		if jobPlan.NextTime.Before(now) || jobPlan.NextTime.Equal(now) {
			s.TryStartJob(jobPlan)
		}

		if nearTime == nil || jobPlan.NextTime.Before(*nearTime) {
			nearTime = &jobPlan.NextTime
		}
	}
	if nearTime != nil {
		scheduleAfter = (*nearTime).Sub(now)
	}
	return
}// 处理任务结果

func (s *Scheduler) handleJobResult(result *common.JobExecuteResult) {
	var (
		jobLog *common.JobLog
	)
	// 删除执行状态
	delete(s.jobExecutingTable, result.ExecuteInfo.Job.Name)

	// 生成执行日志
	if result.Err != common.ERR_LOCK_ALREADY_REQUIRED {
		jobLog = &common.JobLog{
			JobName: result.ExecuteInfo.Job.Name,
			Command: result.ExecuteInfo.Job.Command,
			Output: string(result.Output),
			PlanTime: result.ExecuteInfo.PlanTime.UnixNano() / 1000 / 1000,
			ScheduleTime: result.ExecuteInfo.RealTime.UnixNano() / 1000 / 1000,
			StartTime: result.StartTime.UnixNano() / 1000 / 1000,
			EndTime: result.EndTime.UnixNano() / 1000 / 1000,
		}
		if result.Err != nil {
			jobLog.Err = result.Err.Error()
		} else {
			jobLog.Err = ""
		}
		fmt.Println("job execute finish:", jobLog)
		//G_logSink.Append(jobLog)
	}

	// fmt.Println("任务执行完成:", result.ExecuteInfo.Job.Name, string(result.Output), result.Err)
}

//调度协成
func (s *Scheduler) schedulerLoop()  {
	fmt.Println("schedulerLoop start...")
	var (
		jobEvent *common.JobEvent
		scheduleAfer time.Duration
		scheduleTimer *time.Timer
		jobResult *common.JobExecuteResult
	)

	scheduleAfer = s.TrySchedule()
	scheduleTimer = time.NewTimer(scheduleAfer)

	for {
		select {
		case jobEvent = <- s.jobEventChan:
			s.handleJobEvent(jobEvent)
		case <-scheduleTimer.C:
		case jobResult = <- s.jobResultChan:
			s.handleJobResult(jobResult)
		}
		scheduleAfer = s.TrySchedule()
		scheduleTimer.Reset(scheduleAfer)
	}
}

func (s *Scheduler) PushJobEvent(jobEvent *common.JobEvent)  {
	s.jobEventChan <- jobEvent
}

func InitScheduler() (err error)  {
	GScheduler = &Scheduler{
		jobEventChan:      make(chan *common.JobEvent, 1000),jobPlanTable: make(map[string]*common.JobSchedulePlan),
		jobExecutingTable: make(map[string]*common.JobExecuteInfo),
		jobResultChan: make(chan *common.JobExecuteResult, 1000),
	}

	// start
	go GScheduler.schedulerLoop()
	return
}

func (s *Scheduler) PushJobResult(jobResult *common.JobExecuteResult)  {
	s.jobResultChan <- jobResult
}