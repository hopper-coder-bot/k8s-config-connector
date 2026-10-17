// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// +tool:mockgcp-support
// proto.service: google.cloud.visionai.v1.AppPlatform
// proto.message: google.cloud.visionai.v1.Application

package mockvisionai

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "cloud.google.com/go/visionai/apiv1/visionaipb"
	"github.com/GoogleCloudPlatform/k8s-config-connector/mockgcp/common/projects"
	"github.com/GoogleCloudPlatform/k8s-config-connector/mockgcp/pkg/storage"
)

type visionaiAppPlatform struct {
	*MockService
	pb.UnimplementedAppPlatformServer
}

func (s *visionaiAppPlatform) CreateApplication(ctx context.Context, req *pb.CreateApplicationRequest) (*longrunning.Operation, error) {
	reqName := fmt.Sprintf("%s/applications/%s", req.GetParent(), req.GetApplicationId())
	name, err := s.parseApplicationName(reqName)
	if err != nil {
		return nil, err
	}

	fqn := name.String()
	now := time.Now()

	obj := proto.Clone(req.GetApplication()).(*pb.Application)
	obj.Name = fqn
	obj.CreateTime = timestamppb.New(now)
	obj.UpdateTime = timestamppb.New(now)
	obj.State = pb.Application_CREATED

	if obj.ApplicationConfigs != nil {
		obj.ApplicationConfigs = proto.Clone(obj.ApplicationConfigs).(*pb.ApplicationConfigs)
		obj.ApplicationConfigs.EventDeliveryConfig = nil
	}

	if err := s.storage.Create(ctx, fqn, obj); err != nil {
		return nil, err
	}

	prefix := fmt.Sprintf("projects/%s/locations/%s", name.Project.ID, name.Location)
	metadata := &pb.OperationMetadata{
		ApiVersion:            "v1",
		CreateTime:            timestamppb.New(now),
		RequestedCancellation: false,
		Target:                fqn,
		Verb:                  "create",
	}

	return s.operations.StartLRO(ctx, prefix, metadata, func() (proto.Message, error) {
		metadata.EndTime = timestamppb.Now()
		return obj, nil
	})
}

func (s *visionaiAppPlatform) GetApplication(ctx context.Context, req *pb.GetApplicationRequest) (*pb.Application, error) {
	name, err := s.parseApplicationName(req.GetName())
	if err != nil {
		return nil, err
	}

	fqn := name.String()

	obj := &pb.Application{}
	if err := s.storage.Get(ctx, fqn, obj); err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, status.Errorf(codes.NotFound, "Resource '%s' was not found", fqn)
		}
		return nil, err
	}

	return obj, nil
}

func (s *visionaiAppPlatform) ListApplications(ctx context.Context, req *pb.ListApplicationsRequest) (*pb.ListApplicationsResponse, error) {
	name, err := s.parseApplicationName(req.GetParent() + "/applications/dummy")
	if err != nil {
		return nil, err
	}

	response := &pb.ListApplicationsResponse{}
	findPrefix := strings.TrimSuffix(name.String(), "dummy")
	listKind := (&pb.Application{}).ProtoReflect().Descriptor()

	if err := s.storage.List(ctx, listKind, storage.ListOptions{}, func(obj proto.Message) error {
		app := obj.(*pb.Application)
		if strings.HasPrefix(app.GetName(), findPrefix) {
			response.Applications = append(response.Applications, app)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return response, nil
}

func (s *visionaiAppPlatform) UpdateApplication(ctx context.Context, req *pb.UpdateApplicationRequest) (*longrunning.Operation, error) {
	name, err := s.parseApplicationName(req.GetApplication().GetName())
	if err != nil {
		return nil, err
	}

	fqn := name.String()
	obj := &pb.Application{}
	if err := s.storage.Get(ctx, fqn, obj); err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, status.Errorf(codes.NotFound, "Resource '%s' was not found", fqn)
		}
		return nil, err
	}

	paths := req.GetUpdateMask().GetPaths()
	if len(paths) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "update_mask must be provided")
	}

	for _, path := range paths {
		switch path {
		case "display_name", "displayName":
			obj.DisplayName = req.GetApplication().GetDisplayName()
		case "description":
			obj.Description = req.GetApplication().GetDescription()
		case "labels":
			obj.Labels = req.GetApplication().GetLabels()
		case "application_configs", "applicationConfigs":
			if req.GetApplication().GetApplicationConfigs() != nil {
				configs := proto.Clone(req.GetApplication().GetApplicationConfigs()).(*pb.ApplicationConfigs)
				configs.EventDeliveryConfig = nil
				obj.ApplicationConfigs = configs
			} else {
				obj.ApplicationConfigs = nil
			}
		case "billing_mode", "billingMode":
			obj.BillingMode = req.GetApplication().GetBillingMode()
		default:
			return nil, status.Errorf(codes.InvalidArgument, "field %q is not yet handled in mock", path)
		}
	}

	now := time.Now()
	obj.UpdateTime = timestamppb.New(now)
	obj.RuntimeInfo = &pb.Application_ApplicationRuntimeInfo{}

	if err := s.storage.Update(ctx, fqn, obj); err != nil {
		return nil, err
	}

	prefix := fmt.Sprintf("projects/%s/locations/%s", name.Project.ID, name.Location)
	metadata := &pb.OperationMetadata{
		ApiVersion:            "v1",
		CreateTime:            timestamppb.New(now),
		RequestedCancellation: false,
		Target:                fqn,
		Verb:                  "update",
	}

	return s.operations.StartLRO(ctx, prefix, metadata, func() (proto.Message, error) {
		metadata.EndTime = timestamppb.Now()
		return obj, nil
	})
}

func (s *visionaiAppPlatform) DeleteApplication(ctx context.Context, req *pb.DeleteApplicationRequest) (*longrunning.Operation, error) {
	name, err := s.parseApplicationName(req.GetName())
	if err != nil {
		return nil, err
	}

	fqn := name.String()
	deleted := &pb.Application{}
	if err := s.storage.Delete(ctx, fqn, deleted); err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, status.Errorf(codes.NotFound, "Resource '%s' was not found", fqn)
		}
		return nil, err
	}

	now := time.Now()
	prefix := fmt.Sprintf("projects/%s/locations/%s", name.Project.ID, name.Location)
	metadata := &pb.OperationMetadata{
		ApiVersion:            "v1",
		CreateTime:            timestamppb.New(now),
		RequestedCancellation: false,
		Target:                fqn,
		Verb:                  "delete",
	}

	return s.operations.StartLRO(ctx, prefix, metadata, func() (proto.Message, error) {
		metadata.EndTime = timestamppb.Now()
		return &emptypb.Empty{}, nil
	})
}

type applicationName struct {
	Project     *projects.ProjectData
	Location    string
	Application string
}

func (n *applicationName) String() string {
	return fmt.Sprintf("projects/%s/locations/%s/applications/%s", n.Project.ID, n.Location, n.Application)
}

func (s *MockService) parseApplicationName(name string) (*applicationName, error) {
	tokens := strings.Split(name, "/")
	if len(tokens) == 6 && tokens[0] == "projects" && tokens[2] == "locations" && tokens[4] == "applications" {
		project, err := s.Projects.GetProjectByID(tokens[1])
		if err != nil {
			return nil, err
		}

		return &applicationName{
			Project:     project,
			Location:    tokens[3],
			Application: tokens[5],
		}, nil
	}

	return nil, status.Errorf(codes.InvalidArgument, "invalid application name %q", name)
}
