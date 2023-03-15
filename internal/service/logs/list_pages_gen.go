// Code generated by "internal/generate/listpages/main.go -ListOps=DescribeQueryDefinitions,DescribeResourcePolicies -ContextOnly"; DO NOT EDIT.

package logs

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs/cloudwatchlogsiface"
)

func describeQueryDefinitionsPages(ctx context.Context, conn cloudwatchlogsiface.CloudWatchLogsAPI, input *cloudwatchlogs.DescribeQueryDefinitionsInput, fn func(*cloudwatchlogs.DescribeQueryDefinitionsOutput, bool) bool) error {
	for {
		output, err := conn.DescribeQueryDefinitionsWithContext(ctx, input)
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
func describeResourcePoliciesPages(ctx context.Context, conn cloudwatchlogsiface.CloudWatchLogsAPI, input *cloudwatchlogs.DescribeResourcePoliciesInput, fn func(*cloudwatchlogs.DescribeResourcePoliciesOutput, bool) bool) error {
	for {
		output, err := conn.DescribeResourcePoliciesWithContext(ctx, input)
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