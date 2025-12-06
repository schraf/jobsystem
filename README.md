# jobsystem

A concurrent job execution system for Go with dependency management.

## Overview

`jobsystem` provides a way to schedule and execute jobs concurrently while ensuring that jobs are executed only after their dependencies have completed successfully.

## Installation

```bash
go get github.com/schraf/jobsystem
```

## Usage

```go
package main

import (
    "context"
    "fmt"
    "github.com/schraf/jobsystem"
)

func main() {
    system := jobsystem.NewJobSystem()
    ctx := context.Background()

    // Create job IDs
    job1 := jobsystem.NewJobId()
    job2 := jobsystem.NewJobId()
    job3 := jobsystem.NewJobId()

    // Schedule jobs with dependencies
    system.ScheduleJob(job1, nil, func(ctx context.Context) error {
        fmt.Println("Job 1 executing")
        return nil
    })

    system.ScheduleJob(job2, []jobsystem.JobId{job1}, func(ctx context.Context) error {
        fmt.Println("Job 2 executing (depends on job1)")
        return nil
    })

    system.ScheduleJob(job3, []jobsystem.JobId{job1, job2}, func(ctx context.Context) error {
        fmt.Println("Job 3 executing (depends on job1 and job2)")
        return nil
    })

    // Run all jobs with a concurrency limit of 10
    if err := system.Run(ctx, 10); err != nil {
        fmt.Printf("Error: %v\n", err)
    }
}
```

## Features

- Concurrent job execution with configurable concurrency limits
- Dependency management
- Type-safe job identifiers
- Error handling with custom error types

## Concurrency Limits

The `Run` method requires a `concurrencyLimit` parameter that specifies the maximum number of jobs that can run simultaneously. This limit must be greater than 0. The system uses a semaphore to enforce this limit, ensuring that resource usage is controlled even when many jobs are ready to execute.
