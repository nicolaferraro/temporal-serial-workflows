package serial

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

func TestSimple(t *testing.T) {
	data := make(chan int)
	sendNum := func(ctx workflow.Context, val int) error {
		data <- val
		return nil
	}
	taskQueue := "test-queue"
	c := temporalTestEnv(t, taskQueue, map[string]interface{}{
		"sendNum": sendNum,
	})

	options := workflow.ChildWorkflowOptions{
		TaskQueue: taskQueue,
	}
	num := 23
	_, err := New(c, "myqueue").ExecuteWorkflow(context.Background(), options, "sendNum", num)
	require.NoError(t, err)
	select {
	case val := <-data:
		assert.Equal(t, num, val)
	case <-time.After(20 * time.Second):
		assert.Fail(t, "no value received")
	}
}

func TestSerial(t *testing.T) {
	data := make(chan int, 100)
	sendNums := func(ctx workflow.Context) error {
		for i := 0; i < 5; i++ {
			data <- i
			err := workflow.Sleep(ctx, time.Second)
			require.NoError(t, err)
		}
		return nil
	}
	sendNums2 := func(ctx workflow.Context) error {
		for i := 5; i < 10; i++ {
			data <- i
			err := workflow.Sleep(ctx, time.Second)
			require.NoError(t, err)
		}
		close(data)
		return nil
	}
	taskQueue := "test-queue"
	c := temporalTestEnv(t, taskQueue, map[string]interface{}{
		"sendNums":  sendNums,
		"sendNums2": sendNums2,
	})

	options := workflow.ChildWorkflowOptions{
		TaskQueue: taskQueue,
	}
	_, err := New(c, "myqueue3").ExecuteWorkflow(context.Background(), options, "sendNums", nil)
	_, err = New(c, "myqueue3").ExecuteWorkflow(context.Background(), options, "sendNums2", nil)
	require.NoError(t, err)

	nums := make([]int, 0, 10)
cycle:
	for {
		select {
		case val, ok := <-data:
			if ok {
				nums = append(nums, val)
			} else {
				break cycle
			}
		case <-time.After(20 * time.Second):
			assert.Fail(t, "no value received")
		}
	}
	assert.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, nums)
}

func TestParallel(t *testing.T) {
	data1 := make(chan int)
	data2 := make(chan int)
	data3 := make(chan int)
	sendNum := func(ctx workflow.Context) error {
		data1 <- 1
		v2 := waitForData(t, ctx, data2)
		assert.Equal(t, 2, v2)
		data3 <- 3
		return nil
	}
	sendNum2 := func(ctx workflow.Context) error {
		v1 := waitForData(t, ctx, data1)
		assert.Equal(t, 1, v1)
		data2 <- 2
		return nil
	}
	taskQueue := "test-queue"
	c := temporalTestEnv(t, taskQueue, map[string]interface{}{
		"sendNum":  sendNum,
		"sendNum2": sendNum2,
	})

	options := workflow.ChildWorkflowOptions{
		TaskQueue: taskQueue,
	}
	_, err := New(c, "myqueue1").ExecuteWorkflow(context.Background(), options, "sendNum", nil)
	_, err = New(c, "myqueue2").ExecuteWorkflow(context.Background(), options, "sendNum2", nil)
	require.NoError(t, err)
	select {
	case val := <-data3:
		assert.Equal(t, 3, val)
	case <-time.After(20 * time.Second):
		assert.Fail(t, "no value received")
	}
}

func TestError(t *testing.T) {
	data := make(chan int)
	fail := func(ctx workflow.Context) error {
		data <- 1
		return errors.New("myerror")
	}
	taskQueue := "test-queue"
	c := temporalTestEnv(t, taskQueue, map[string]interface{}{
		"fail": fail,
	})

	options := workflow.ChildWorkflowOptions{
		TaskQueue: taskQueue,
	}
	for i := 0; i < 5; i++ {
		r, err := New(c, "myqueue").ExecuteWorkflow(context.Background(), options, "fail", nil)
		require.NoError(t, err)
		select {
		case val := <-data:
			assert.Equal(t, 1, val)
		case <-time.After(20 * time.Second):
			assert.Fail(t, "no value received")
		}
		ctx := context.Background()
		err = r.Get(ctx, nil)
		assert.Error(t, err)
	}

}

func temporalTestEnv(t *testing.T, taskQueue string, workflows map[string]interface{}) client.Client {
	interrupt := make(chan interface{})
	c, err := client.NewClient(client.Options{})
	require.NoError(t, err)

	t.Cleanup(func() {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
		defer cancel()
		for i := 0; i < 5; i++ {
			res, err := c.ListOpenWorkflow(ctx, &workflowservice.ListOpenWorkflowExecutionsRequest{})
			require.NoError(t, err)
			if res.Size() == 0 {
				time.Sleep(500 * time.Millisecond)
				interrupt <- 0
				return
			}
			time.Sleep(time.Second)
		}
		require.Fail(t, "cannot drain work queue")
	})

	w := worker.New(c, taskQueue, worker.Options{
		EnableSessionWorker: true,
	})
	for k, v := range workflows {
		w.RegisterWorkflowWithOptions(v, workflow.RegisterOptions{
			Name: k,
		})
	}
	w.RegisterWorkflow(SerialWorkflow)
	go func() {
		err = w.Run(interrupt)
		require.NoError(t, err)
	}()
	return c
}

// waitForData wait up to 10 secs to get a message, without making temporal think of a deadlock
func waitForData(t *testing.T, ctx workflow.Context, channel <-chan int) int {
	for i := 0; i < 10; i++ {
		select {
		case v, ok := <-channel:
			if ok {
				return v
			} else {
				require.Fail(t, "channel closed")
				return 0
			}
		default:
			err := workflow.Sleep(ctx, time.Second)
			require.NoError(t, err)
		}
	}
	require.Fail(t, "no data")
	return 0
}
