/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package grpc

import (
    "fmt"

    "golang.org/x/net/context"
    "google.golang.org/grpc"

    rpb "google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
    "github.com/gogo/protobuf/proto"
    "github.com/golang/protobuf/protoc-gen-go/descriptor"
//    protobufdescriptor "github.com/golang/protobuf/descriptor"
)

const reflectionServiceName = "grpc.reflection.v1alpha.ServerReflection"

type DescriptorClient interface {
    GetServices() (GrpcServices, error)
}

type DescriptorClientImpl struct {
    conn *grpc.ClientConn
    client rpb.ServerReflectionClient
}

func NewDescriptorClient(conn *grpc.ClientConn) DescriptorClient {
    return &DescriptorClientImpl{conn, rpb.NewServerReflectionClient(conn)}
}

type GrpcServices struct {
    Services map[string]*descriptor.ServiceDescriptorProto
    Messages map[string]*descriptor.DescriptorProto
}

func (c *DescriptorClientImpl) getReflectionResponse(stream rpb.ServerReflection_ServerReflectionInfoClient) (
    *rpb.ServerReflectionResponse, error){

    if err := stream.Send(&rpb.ServerReflectionRequest{
        MessageRequest: &rpb.ServerReflectionRequest_ListServices{},
    }); err != nil {
        return nil, fmt.Errorf("failed to send request: %v", err)
    }
    r, err := stream.Recv()
    if err != nil {
        // io.EOF is not ok.
        return nil, fmt.Errorf("failed to recv response: %v", err)
    }
    return r, nil
}

func (c *DescriptorClientImpl) GetServices() (GrpcServices, error) {
    stream, err := c.client.ServerReflectionInfo(context.Background(), grpc.FailFast(false))
    if err != nil {
        return GrpcServices{}, fmt.Errorf("cannot get ServerReflectionInfo: %v", err)
    }

    result := GrpcServices{
        map[string]*descriptor.ServiceDescriptorProto{},
        map[string]*descriptor.DescriptorProto{},
    }

    r, err := c.getReflectionResponse(stream)
    if err != nil {
        return GrpcServices{}, err
    }

    for _, s := range r.GetListServicesResponse().Service {
        // Don't lookup reflection service
        if s.Name == reflectionServiceName {
            continue
        }
        // Skip services we have already found in previous requests
        if _, found := result.Services[fmt.Sprintf(".%s", s.Name)]; found {
            continue
        }

        // Lookup the FileDescriptorProto containing the service
        if err := stream.Send(&rpb.ServerReflectionRequest{
            MessageRequest: &rpb.ServerReflectionRequest_FileContainingSymbol{
                FileContainingSymbol: s.Name,
            },
        }); err != nil {
            return GrpcServices{}, fmt.Errorf("failed to send request: %v", err)
        }
        r, err = stream.Recv()
        if err != nil {
            // io.EOF is not ok.
            return GrpcServices{}, fmt.Errorf("failed to recv response: %v", err)
        }

        // Index the Services and Messages in the FileDescriptorProto
        switch r.MessageResponse.(type) {
        case *rpb.ServerReflectionResponse_FileDescriptorResponse:
            for _, b := range r.GetFileDescriptorResponse().GetFileDescriptorProto() {
                fdp := descriptor.FileDescriptorProto{}
                err := proto.Unmarshal(b, &fdp)
                if err != nil {
                    // io.EOF is not ok.
                    return GrpcServices{}, fmt.Errorf("failed to unmarshal response: %v", err)
                }
                for _, s := range fdp.GetService() {
                    result.Services[fmt.Sprintf(".%s.%s", *fdp.Package, *s.Name)] = s
                }
                for _, m := range fdp.GetMessageType() {
                    result.Messages[fmt.Sprintf(".%s.%s", *fdp.Package, *m.Name)] = m
                }
            }
        }
    }
    return result, nil
}

