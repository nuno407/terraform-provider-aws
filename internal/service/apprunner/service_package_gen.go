// Code generated by internal/generate/servicepackages/main.go; DO NOT EDIT.

package apprunner

import (
	"context"

	"github.com/hashicorp/terraform-provider-aws/internal/types"
	"github.com/hashicorp/terraform-provider-aws/names"
)

type servicePackage struct{}

func (p *servicePackage) FrameworkDataSources(ctx context.Context) []*types.ServicePackageFrameworkDataSource {
	return []*types.ServicePackageFrameworkDataSource{}
}

func (p *servicePackage) FrameworkResources(ctx context.Context) []*types.ServicePackageFrameworkResource {
	return []*types.ServicePackageFrameworkResource{}
}

func (p *servicePackage) SDKDataSources(ctx context.Context) []*types.ServicePackageSDKDataSource {
	return []*types.ServicePackageSDKDataSource{}
}

func (p *servicePackage) SDKResources(ctx context.Context) []*types.ServicePackageSDKResource {
	return []*types.ServicePackageSDKResource{
		{
			Factory:  ResourceAutoScalingConfigurationVersion,
			TypeName: "aws_apprunner_auto_scaling_configuration_version",
		},
		{
			Factory:  ResourceConnection,
			TypeName: "aws_apprunner_connection",
		},
		{
			Factory:  ResourceCustomDomainAssociation,
			TypeName: "aws_apprunner_custom_domain_association",
		},
		{
			Factory:  ResourceObservabilityConfiguration,
			TypeName: "aws_apprunner_observability_configuration",
		},
		{
			Factory:  ResourceService,
			TypeName: "aws_apprunner_service",
		},
		{
			Factory:  ResourceVPCConnector,
			TypeName: "aws_apprunner_vpc_connector",
		},
		{
			Factory:  ResourceVPCIngressConnection,
			TypeName: "aws_apprunner_vpc_ingress_connection",
		},
	}
}

func (p *servicePackage) ServicePackageName() string {
	return names.AppRunner
}

var ServicePackage = &servicePackage{}