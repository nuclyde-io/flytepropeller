package fetcher

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/ptypes"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/admin"
	"github.com/lyft/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	"github.com/lyft/flytepropeller/pkg/client/clientset/versioned"
	"github.com/lyft/flytepropeller/pkg/compiler/transformers/k8s"
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

func (p processor) ProcessDelete(ctx context.Context, d *admin.TerminateExecutionAction) error {
	logger.Infof(ctx, "delete workflow received, for [%s]", d.String())
	// TODO Add namespace
	err := p.workflowClient.FlyteworkflowV1alpha1().FlyteWorkflows(fmt.Sprintf("%s-%s", d.Id.Project, d.Id.Domain)).Delete(d.Id.Name, &v1.DeleteOptions{
		PropagationPolicy: &deletePropagationBackground,
	})
	if err != nil && kubeerrors.IsNotFound(err) {
		// Ignore already processed
		return nil
	} else {
		return err
	}
	return nil
}

func (p processor) ProcessCreate(ctx context.Context, startAction *admin.StartExecutionAction) error {
	logger.Infof(ctx, "create received for [%s]", startAction.Id.String())
	// TODO Add namespace
	wf, err := k8s.BuildFlyteWorkflow(startAction.Closure, startAction.Inputs, startAction.Id, fmt.Sprintf("%s-%s", startAction.Id.Project, startAction.Id.Domain))
	if err != nil {
		return err
	}

	wf.Labels = startAction.ExecMetadata.Labels.Values
	wf.Annotations = startAction.ExecMetadata.Annotations.Values
	t, err := ptypes.Timestamp(startAction.ExecMetadata.AcceptedAt)
	if err != nil {
		return err
	}
	wf.AcceptedAt = &v1.Time{
		Time: t,
	}
	if startAction.ExecMetadata.AuthRole != nil && startAction.ExecMetadata.AuthRole.GetKubernetesServiceAccount() != nil {
		wf.ServiceAccountName = startAction.ExecMetadata.AuthRole.GetKubernetesServiceAccount()
	}
	startAction.ExecSysOverrides.TaskPluginOverrides
	_, err = p.workflowClient.FlyteworkflowV1alpha1().FlyteWorkflows(wf.Namespace).Create(wf)
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
	err = p.ProcessCreate(ctx, wf)
	// TODO ERR should cause an error event to Admin
	return err
}
