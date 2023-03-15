// Code generated by internal/generate/tags/main.go; DO NOT EDIT.
package iam

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/iam"
	tftags "github.com/hashicorp/terraform-provider-aws/internal/tags"
)

// []*SERVICE.Tag handling

// Tags returns iam service tags.
func Tags(tags tftags.KeyValueTags) []*iam.Tag {
	result := make([]*iam.Tag, 0, len(tags))

	for k, v := range tags.Map() {
		tag := &iam.Tag{
			Key:   aws.String(k),
			Value: aws.String(v),
		}

		result = append(result, tag)
	}

	return result
}

// KeyValueTags creates tftags.KeyValueTags from iam service tags.
func KeyValueTags(ctx context.Context, tags []*iam.Tag) tftags.KeyValueTags {
	m := make(map[string]*string, len(tags))

	for _, tag := range tags {
		m[aws.StringValue(tag.Key)] = tag.Value
	}

	return tftags.New(ctx, m)
}