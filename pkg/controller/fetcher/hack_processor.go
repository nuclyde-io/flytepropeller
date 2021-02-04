package fetcher

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/lyft/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	"github.com/lyft/flytepropeller/pkg/client/clientset/versioned"
	"github.com/lyft/flytestdlib/logger"
	kubeerrors "k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type DeleteMessage struct {
	// The following field exists just to disambiguate the 2 messages
	DummyField int `json:"dummy_field"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

func (d DeleteMessage) String() string {
	return fmt.Sprintf("%s/%s", d.Namespace, d.Name)
}

type processor struct {
	workflowClient versioned.Interface
}

var deletePropagationBackground = v1.DeletePropagationBackground

func (p processor) ProcessDelete(ctx context.Context, d DeleteMessage) error {
	logger.Infof(ctx, "delete workflow received, for [%s]", d.String())
	err := p.workflowClient.FlyteworkflowV1alpha1().FlyteWorkflows(d.Namespace).Delete(d.Name, &v1.DeleteOptions{
		PropagationPolicy: &deletePropagationBackground,
	})
	if err != nil && kubeerrors.IsNotFound(err) {
		// Ignore already processed
		return nil
	}
	return nil
}

func (p processor) ProcessCreate(ctx context.Context, wf *v1alpha1.FlyteWorkflow) error {
	logger.Infof(ctx, "create received for [%s]", wf.ExecutionID.String())
	_, err := p.workflowClient.FlyteworkflowV1alpha1().FlyteWorkflows(wf.Namespace).Create(wf)
	if err != nil && kubeerrors.IsAlreadyExists(err) {
		// Already exists, ignore
		return nil
	}
	return err
}

func (p processor) Process(ctx context.Context, m []byte) error {
	d := DeleteMessage{}
	err := json.Unmarshal(m, &d)
	if err == nil {
		return p.ProcessDelete(ctx, d)
	}

	// lets try if the message is not Delete
	wf := &v1alpha1.FlyteWorkflow{}
	err = json.Unmarshal(m, wf)
	if err != nil {
		logger.Errorf(ctx, "Failed to unmarshal as delete or create messages - unknown message received, err: %s", err)
		return err
	}
	return p.ProcessCreate(ctx, wf)
}
