// Code generated by "internal/generate/listpages/main.go -ListOps=DescribeCapacityProviders -ContextOnly"; DO NOT EDIT.

package ecs

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ecs"
)

func describeCapacityProvidersPages(ctx context.Context, conn *ecs.ECS, input *ecs.DescribeCapacityProvidersInput, fn func(*ecs.DescribeCapacityProvidersOutput, bool) bool) error {
	for {
		output, err := conn.DescribeCapacityProvidersWithContext(ctx, input)
		if err != nil {
			return err
		}

		lastPage := aws.StringValue(output.NextToken) == ""
		if !fn(output, lastPage) || lastPage {
			break
		}

		input.NextToken = output.NextToken
	}
	return nil
}