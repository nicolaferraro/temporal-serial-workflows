package serial

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestSimpleExecution(t *testing.T) {
	data := make(chan int)
	c := newUnitTestEnv(map[string]interface{}{
		"sendNum": func(ctx workflow.Context, val int) error {
			data <- val
			return nil
		},
	})
	num := 23
	_, err := New(c, "myqueue").ExecuteWorkflow(context.Background(), workflow.ChildWorkflowOptions{}, "sendNum", num)
	require.NoError(t, err)
	select {
	case val := <-data:
		assert.Equal(t, num, val)
	case <-time.After(20 * time.Second):
		require.Fail(t, "no value received")
	}
}

func TestMultipleExecutionsAreSerialized(t *testing.T) {
	data := make(chan int, 100)
	c := newUnitTestEnv(map[string]interface{}{
		"sendNums": func(ctx workflow.Context) error {
			for i := 0; i < 5; i++ {
				data <- i
				time.Sleep(50 * time.Millisecond)
			}
			return nil
		},
		"sendNums2": func(ctx workflow.Context) error {
			for i := 5; i < 10; i++ {
				data <- i
				time.Sleep(50 * time.Millisecond)
			}
			close(data)
			return nil
		},
	})
	_, err := New(c, "myqueue3").ExecuteWorkflow(context.Background(), workflow.ChildWorkflowOptions{}, "sendNums", nil)
	require.NoError(t, err)
	_, err = New(c, "myqueue3").ExecuteWorkflow(context.Background(), workflow.ChildWorkflowOptions{}, "sendNums2", nil)
	require.NoError(t, err)

	nums := make([]int, 0, 10)
	end := false
	for !end {
		select {
		case val, ok := <-data:
			if ok {
				nums = append(nums, val)
			} else {
				end = true
			}
		case <-time.After(5 * time.Second):
			require.Fail(t, "no value received")
		}
	}
	assert.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, nums)
}

func TestErrorsDoNotStopSubsequentExecutions(t *testing.T) {
	data := make(chan int, 100)
	c := newUnitTestEnv(map[string]interface{}{
		"fail": func(ctx workflow.Context) error {
			data <- 1
			return errors.New("myerror")
		},
	})
	for i := 0; i < 5; i++ {
		_, err := New(c, "myqueue").ExecuteWorkflow(context.Background(), workflow.ChildWorkflowOptions{}, "fail", nil)
		require.NoError(t, err)
	}

	for i := 0; i < 5; i++ {
		select {
		case val := <-data:
			assert.Equal(t, 1, val)
		case <-time.After(20 * time.Second):
			require.Fail(t, "no value received")
		}
	}
}

type unitTestEnv struct {
	*testsuite.TestWorkflowEnvironment
	launched bool
}

func (env *unitTestEnv) SignalWithStartWorkflow(ctx context.Context, workflowID string, signalName string, signalArg interface{},
	options client.StartWorkflowOptions, workflow interface{}, workflowArgs ...interface{}) (client.WorkflowRun, error) {
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalName, signalArg)
	}, 0)

	if !env.launched || env.IsWorkflowCompleted() {
		env.launched = true
		go env.ExecuteWorkflow(workflow, workflowArgs...)
	}
	return mockWorkflowRun{id: workflowID}, nil
}

var _ SignalingClient = &unitTestEnv{}

type mockWorkflowRun struct {
	id string
}

func (m mockWorkflowRun) GetID() string {
	return m.id
}

func (m mockWorkflowRun) GetRunID() string {
	return "mock"
}

func (m mockWorkflowRun) Get(ctx context.Context, valuePtr interface{}) error {
	return nil
}

var _ client.WorkflowRun = mockWorkflowRun{}

func newUnitTestEnv(workflows map[string]interface{}) *unitTestEnv {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	for k, v := range workflows {
		env.RegisterWorkflowWithOptions(v, workflow.RegisterOptions{
			Name: k,
		})
	}
	env.RegisterWorkflow(SerialWorkflow)

	return &unitTestEnv{TestWorkflowEnvironment: env}
}
