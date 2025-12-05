package jobsystem

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
)

//--=========================================================--
//--== Job States
//--=========================================================--

const (
	waiting  int32 = 0
	blocked  int32 = 1
	running  int32 = 2
	finished int32 = 3
)

//--=========================================================--
//--== Job Function
//--=========================================================--

type JobFunc func(ctx context.Context) error

//--=========================================================--
//--== Job Identifier
//--=========================================================--

type JobId uuid.UUID

func NewJobId() JobId {
	return JobId(uuid.New())
}

func (j JobId) String() string {
	return uuid.UUID(j).String()
}

//--=========================================================--
//--== Errors
//--=========================================================--

type JobDependencyError struct {
	Dependent  JobId
	Dependency JobId
}

func (e *JobDependencyError) Error() string {
	return fmt.Sprintf("job %s depends on unknown job %s", e.Dependent.String(), e.Dependency.String())
}

func (e *JobDependencyError) Is(target error) bool {
	t, ok := target.(*JobDependencyError)
	if !ok {
		return false
	}

	return e.Dependent == t.Dependent && e.Dependency == t.Dependency
}

type JobAlreadyScheduledError struct {
	Job JobId
}

func (e *JobAlreadyScheduledError) Error() string {
	return fmt.Sprintf("job %s has already been scheduled", e.Job)
}

func (e *JobAlreadyScheduledError) Is(target error) bool {
	t, ok := target.(*JobAlreadyScheduledError)
	if !ok {
		return false
	}

	return e.Job == t.Job
}

type JobSystemAlreadyRunning struct{}

func (e *JobSystemAlreadyRunning) Error() string {
	return "job system already running"
}

//--=========================================================--
//--== Job System
//--=========================================================--

type job struct {
	state        atomic.Int32
	dependencies []JobId
	function     JobFunc
}

type JobSystem struct {
	jobs    map[JobId]*job
	lock    sync.RWMutex
	running atomic.Bool
}

func NewJobSystem() *JobSystem {
	return &JobSystem{
		jobs: make(map[JobId]*job),
	}
}

func (s *JobSystem) ScheduleJob(id JobId, dependencies []JobId, function JobFunc) error {
	if err := s.validateDependencies(id, dependencies); err != nil {
		return err
	}

	if err := s.insertJob(id, dependencies, function); err != nil {
		return err
	}

	return nil
}

func (s *JobSystem) Run(ctx context.Context) error {
	if !s.running.CompareAndSwap(false, true) {
		return &JobSystemAlreadyRunning{}
	}
	defer s.running.Store(false)

	var workers sync.WaitGroup
	var startReadyJobs func()
	var err atomic.Value

	startReadyJobs = func() {
		for readyJob := s.acquireJob(); readyJob != nil; readyJob = s.acquireJob() {
			workers.Add(1)

			go func(work *job) {
				defer workers.Done()

				work.state.Store(running)

				if jobError := work.function(ctx); jobError != nil {
					err.CompareAndSwap(nil, jobError)
				}

				work.state.Store(finished)

				if err.Load() == nil {
					startReadyJobs()
				}
			}(readyJob)

			if err.Load() != nil {
				break
			}
		}
	}

	startReadyJobs()
	workers.Wait()

	if err := err.Load(); err != nil {
		return err.(error)
	}

	return nil
}

func (s *JobSystem) validateDependencies(dependent JobId, dependencies []JobId) error {
	s.lock.RLock()
	defer s.lock.RUnlock()

	for _, dependency := range dependencies {
		if _, exists := s.jobs[dependency]; !exists {
			return &JobDependencyError{
				Dependent:  dependent,
				Dependency: dependency,
			}
		}
	}

	return nil
}

func (s *JobSystem) insertJob(id JobId, dependencies []JobId, function JobFunc) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, exists := s.jobs[id]; exists {
		return &JobAlreadyScheduledError{
			Job: id,
		}
	}

	s.jobs[id] = &job{
		dependencies: dependencies,
		function:     function,
	}

	return nil
}

func (s *JobSystem) acquireJob() *job {
	s.lock.RLock()
	defer s.lock.RUnlock()

	for _, job := range s.jobs {
		if job.state.CompareAndSwap(waiting, blocked) {
			ready := true

			for _, dependency := range job.dependencies {
				dependent := s.jobs[dependency]

				if dependent.state.Load() != finished {
					ready = false
					break
				}
			}

			if ready {
				return job
			}

			job.state.Store(waiting)
		}
	}

	return nil
}
