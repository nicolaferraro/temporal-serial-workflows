package serial

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"strings"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
)

const (
	serialSignalName = "serial-start-workflow"
)

type SerialOptions struct {
	client client.Client
	queue  string
}

func New(c client.Client, queue string) *SerialOptions {
	return &SerialOptions{
		client: c,
		queue:  queue,
	}
}

func (o *SerialOptions) ExecuteWorkflow(ctx context.Context, childOptions workflow.ChildWorkflowOptions, workflowFunc interface{}, data interface{}) (client.WorkflowRun, error) {
	name, err := getWorkflowFunctionName(workflowFunc)
	if err != nil {
		return nil, fmt.Errorf("could not determine workflow function name: %w", err)
	}
	childWorkflowDefinition := workflowDefinition{
		Options: childOptions,
		Name:    name,
		Data:    data,
	}

	id := fmt.Sprintf("serial-%s", o.queue)
	options := client.StartWorkflowOptions{
		ID:        id,
		TaskQueue: childOptions.TaskQueue,
	}
	return o.client.SignalWithStartWorkflow(ctx, id, serialSignalName, childWorkflowDefinition, options, SerialWorkflow)
}

type workflowDefinition struct {
	Options workflow.ChildWorkflowOptions
	Name    string
	Data    interface{}
}

func SerialWorkflow(ctx workflow.Context) error {
	var childWorkflowDefinition workflowDefinition
	signalChan := workflow.GetSignalChannel(ctx, serialSignalName)
	var globalErr error
	for signalChan.ReceiveAsync(&childWorkflowDefinition) {
		fut := workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, childWorkflowDefinition.Options), childWorkflowDefinition.Name, childWorkflowDefinition.Data)
		if err := fut.Get(ctx, nil); err != nil {
			if globalErr == nil {
				globalErr = err
			} else {
				globalErr = fmt.Errorf("error while executing child workflow: %v. Previous error: %w", err, globalErr)
			}
		}
	}
	return globalErr
}

func getWorkflowFunctionName(workflowFunc interface{}) (string, error) {
	fnName := ""
	fType := reflect.TypeOf(workflowFunc)
	switch getKind(fType) {
	case reflect.String:
		fnName = reflect.ValueOf(workflowFunc).String()
	case reflect.Func:
		fnName = getFunctionName(workflowFunc)
	default:
		return "", fmt.Errorf("invalid type 'workflowFunc' parameter provided, it can be either worker function or function name: %v", workflowFunc)
	}

	return fnName, nil
}

func getKind(fType reflect.Type) reflect.Kind {
	if fType == nil {
		return reflect.Invalid
	}
	return fType.Kind()
}

func getFunctionName(i interface{}) string {
	if fullName, ok := i.(string); ok {
		return fullName
	}
	fullName := runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
	elements := strings.Split(fullName, ".")
	shortName := elements[len(elements)-1]
	// This allows to call activities by method pointer
	// Compiler adds -fm suffix to a function name which has a receiver
	// Note that this works even if struct pointer used to get the function is nil
	// It is possible because nil receivers are allowed.
	// For example:
	// var a *Activities
	// ExecuteActivity(ctx, a.Foo)
	// will call this function which is going to return "Foo"
	return strings.TrimSuffix(shortName, "-fm")
}
