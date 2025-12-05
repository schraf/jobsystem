package jobsystem

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var ErrTestJob = errors.New("test job error")

type TestJob struct {
	Id        JobId
	DidRun    bool
	StartTime time.Time
	EndTime   time.Time
	Duration  time.Duration
}

func NewTestJob(duration time.Duration) TestJob {
	return TestJob{
		Id: NewJobId(),
	}
}

func (j *TestJob) ScheduleSuccess(system *JobSystem, dependencies []JobId) error {
	return system.ScheduleJob(j.Id, dependencies, func(ctx context.Context) error {
		j.StartTime = time.Now()
		time.Sleep(j.Duration)
		j.EndTime = time.Now()
		j.DidRun = true
		return nil
	})
}

func (j *TestJob) ScheduleFailure(system *JobSystem, dependencies []JobId) error {
	return system.ScheduleJob(j.Id, dependencies, func(ctx context.Context) error {
		j.StartTime = time.Now()
		time.Sleep(j.Duration)
		j.EndTime = time.Now()
		j.DidRun = true
		return ErrTestJob
	})
}

func TestJobSystem_SingleSuccess(t *testing.T) {
	var err error

	ctx := context.Background()

	system := NewJobSystem()

	job := NewTestJob(1 * time.Millisecond)
	err = job.ScheduleSuccess(system, nil)
	assert.NoError(t, err)

	err = system.Run(ctx)
	assert.NoError(t, err)

	assert.True(t, job.DidRun)
}

func TestJobSystem_SingleFailure(t *testing.T) {
	var err error

	ctx := context.Background()

	system := NewJobSystem()

	job := NewTestJob(1 * time.Millisecond)
	err = job.ScheduleFailure(system, nil)
	assert.NoError(t, err)

	err = system.Run(ctx)
	assert.Equal(t, err, ErrTestJob)

	assert.True(t, job.DidRun)
}

func TestJobSystem_DependencyChain(t *testing.T) {
	var err error

	ctx := context.Background()

	system := NewJobSystem()

	jobA := NewTestJob(100 * time.Millisecond)
	jobB := NewTestJob(100 * time.Millisecond)

	err = jobA.ScheduleSuccess(system, nil)
	assert.NoError(t, err)

	err = jobB.ScheduleSuccess(system, []JobId{jobA.Id})
	assert.NoError(t, err)

	err = system.Run(ctx)
	assert.NoError(t, err)

	assert.True(t, jobA.DidRun)
	assert.True(t, jobB.DidRun)
	assert.Greater(t, jobB.StartTime, jobA.EndTime)
}

func TestJobSystem_Concurrency(t *testing.T) {
	var err error

	ctx := context.Background()

	system := NewJobSystem()

	jobA := NewTestJob(100 * time.Millisecond)
	jobB := NewTestJob(100 * time.Millisecond)

	err = jobA.ScheduleSuccess(system, nil)
	assert.NoError(t, err)

	err = jobB.ScheduleSuccess(system, nil)
	assert.NoError(t, err)

	err = system.Run(ctx)
	assert.NoError(t, err)

	assert.True(t, jobA.DidRun)
	assert.True(t, jobB.DidRun)
	assert.Less(t, jobB.StartTime, jobA.EndTime)
	assert.Less(t, jobA.StartTime, jobB.EndTime)
}
