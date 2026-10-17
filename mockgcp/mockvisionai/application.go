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

package mockvisionai

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	pb "cloud.google.com/go/visionai/apiv1/visionaipb"
	"github.com/GoogleCloudPlatform/k8s-config-connector/mockgcp/common/fields"
)

type AppPlatformServer struct {
	*MockService
	pb.UnimplementedAppPlatformServer
}

func (s *AppPlatformServer) GetApplication(ctx context.Context, req *pb.GetApplicationRequest) (*pb.Application, error) {
	name, err := s.parseApplicationName(req.Name)
	if err != nil {
		return nil, err
	}

	fqn := name.String()
	obj := &pb.Application{}
	if err := s.storage.Get(ctx, fqn, obj); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, status.Errorf(codes.NotFound, "Resource '%s' was not found", fqn)
		}
		return nil, err
	}

	return obj, nil
}

func (s *AppPlatformServer) CreateApplication(ctx context.Context, req *pb.CreateApplicationRequest) (*longrunning.Operation, error) {
	reqName := req.Parent + "/applications/" + req.ApplicationId
	name, err := s.parseApplicationName(reqName)
	if err != nil {
		return nil, err
	}

	fqn := name.String()
	now := time.Now()

	obj := proto.Clone(req.Application).(*pb.Application)
	obj.Name = fqn
	obj.CreateTime = timestamppb.New(now)
	obj.UpdateTime = timestamppb.New(now)
	obj.State = pb.Application_CREATED
	if obj.ApplicationConfigs != nil {
		obj.ApplicationConfigs = &pb.ApplicationConfigs{}
	}

	if err := s.storage.Create(ctx, fqn, obj); err != nil {
		return nil, err
	}

	lroMetadata := &pb.OperationMetadata{
		ApiVersion: "v1",
		CreateTime: timestamppb.New(now),
		Target:     fqn,
		Verb:       "create",
	}

	prefix := fmt.Sprintf("projects/%s/locations/%s", name.Project.ID, name.Location)
	return s.operations.StartLRO(ctx, prefix, lroMetadata, func() (proto.Message, error) {
		lroMetadata.EndTime = timestamppb.New(time.Now())
		return obj, nil
	})
}

func (s *AppPlatformServer) UpdateApplication(ctx context.Context, req *pb.UpdateApplicationRequest) (*longrunning.Operation, error) {
	name, err := s.parseApplicationName(req.Application.Name)
	if err != nil {
		return nil, err
	}

	fqn := name.String()
	now := time.Now()

	existing := &pb.Application{}
	if err := s.storage.Get(ctx, fqn, existing); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, status.Errorf(codes.NotFound, "Resource '%s' was not found", fqn)
		}
		return nil, err
	}

	updated := proto.Clone(existing).(*pb.Application)
	if req.UpdateMask != nil && len(req.UpdateMask.Paths) > 0 {
		if err := fields.UpdateByFieldMask(updated, req.Application, req.UpdateMask.Paths); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to update Application fields: %v", err)
		}
	} else {
		updated = proto.Clone(req.Application).(*pb.Application)
	}

	updated.Name = fqn
	updated.UpdateTime = timestamppb.New(now)
	if updated.ApplicationConfigs != nil {
		updated.ApplicationConfigs = &pb.ApplicationConfigs{}
	}
	if updated.RuntimeInfo == nil {
		updated.RuntimeInfo = &pb.Application_ApplicationRuntimeInfo{}
	}

	if err := s.storage.Update(ctx, fqn, updated); err != nil {
		return nil, err
	}

	lroMetadata := &pb.OperationMetadata{
		ApiVersion: "v1",
		CreateTime: timestamppb.New(now),
		Target:     fqn,
		Verb:       "update",
	}

	prefix := fmt.Sprintf("projects/%s/locations/%s", name.Project.ID, name.Location)
	return s.operations.StartLRO(ctx, prefix, lroMetadata, func() (proto.Message, error) {
		lroMetadata.EndTime = timestamppb.New(time.Now())
		return updated, nil
	})
}

func (s *AppPlatformServer) DeleteApplication(ctx context.Context, req *pb.DeleteApplicationRequest) (*longrunning.Operation, error) {
	name, err := s.parseApplicationName(req.Name)
	if err != nil {
		return nil, err
	}

	fqn := name.String()
	oldObj := &pb.Application{}
	if err := s.storage.Delete(ctx, fqn, oldObj); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, status.Errorf(codes.NotFound, "Resource '%s' was not found", fqn)
		}
		return nil, err
	}

	now := time.Now()
	lroMetadata := &pb.OperationMetadata{
		ApiVersion: "v1",
		CreateTime: timestamppb.New(now),
		Target:     fqn,
		Verb:       "delete",
	}

	prefix := fmt.Sprintf("projects/%s/locations/%s", name.Project.ID, name.Location)
	return s.operations.StartLRO(ctx, prefix, lroMetadata, func() (proto.Message, error) {
		lroMetadata.EndTime = timestamppb.New(time.Now())
		return &emptypb.Empty{}, nil
	})
}
