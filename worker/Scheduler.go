package worker

import (
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
	G_scheduler *Scheduler
)
// 尝试执行任务
func (scheduler *Scheduler) TryStartJob(jobPlan *common.JobSchedulePlan) {
	// 调度 和 执行 是2件事情
}


//重新计算任务调度状态
func (s *Scheduler) TrySchedule() (scheduleAfter time.Duration)  {
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
}

//调度协成
func (s *Scheduler) schedulerLoop()  {
	var (
		jobEvent *common.JobEvent
		scheduleAfer time.Duration
		scheduleTimer *time.Time
		jobResult *common.JobExecuteResult
	)
}

func InitScheduler() (err error)  {
	G_scheduler = &Scheduler{
		jobEventChan:      make(chan *common.JobEvent, 1000),jobPlanTable: make(map[string]*common.JobSchedulePlan),
		jobExecutingTable: make(map[string]*common.JobExecuteInfo),
		jobResultChan: make(chan *common.JobExecuteResult, 1000),
	}

	// start
	go G_scheduler.
}