// Code generated by internal/generate/servicepackages/main.go; DO NOT EDIT.

package ec2

import (
	"context"

	"github.com/hashicorp/terraform-provider-aws/internal/types"
	"github.com/hashicorp/terraform-provider-aws/names"
)

type servicePackage struct{}

func (p *servicePackage) FrameworkDataSources(ctx context.Context) []*types.ServicePackageFrameworkDataSource {
	return []*types.ServicePackageFrameworkDataSource{
		{
			Factory: newDataSourceSecurityGroupRule,
		},
		{
			Factory: newDataSourceSecurityGroupRules,
		},
	}
}

func (p *servicePackage) FrameworkResources(ctx context.Context) []*types.ServicePackageFrameworkResource {
	return []*types.ServicePackageFrameworkResource{
		{
			Factory: newResourceSecurityGroupEgressRule,
		},
		{
			Factory: newResourceSecurityGroupIngressRule,
		},
	}
}

func (p *servicePackage) SDKDataSources(ctx context.Context) []*types.ServicePackageSDKDataSource {
	return []*types.ServicePackageSDKDataSource{
		{
			Factory:  DataSourceAMI,
			TypeName: "aws_ami",
		},
		{
			Factory:  DataSourceAMIIDs,
			TypeName: "aws_ami_ids",
		},
		{
			Factory:  DataSourceAvailabilityZone,
			TypeName: "aws_availability_zone",
		},
		{
			Factory:  DataSourceAvailabilityZones,
			TypeName: "aws_availability_zones",
		},
		{
			Factory:  DataSourceCustomerGateway,
			TypeName: "aws_customer_gateway",
		},
		{
			Factory:  DataSourceEBSDefaultKMSKey,
			TypeName: "aws_ebs_default_kms_key",
		},
		{
			Factory:  DataSourceEBSEncryptionByDefault,
			TypeName: "aws_ebs_encryption_by_default",
		},
		{
			Factory:  DataSourceEBSSnapshot,
			TypeName: "aws_ebs_snapshot",
		},
		{
			Factory:  DataSourceEBSSnapshotIDs,
			TypeName: "aws_ebs_snapshot_ids",
		},
		{
			Factory:  DataSourceEBSVolume,
			TypeName: "aws_ebs_volume",
		},
		{
			Factory:  DataSourceEBSVolumes,
			TypeName: "aws_ebs_volumes",
		},
		{
			Factory:  DataSourceClientVPNEndpoint,
			TypeName: "aws_ec2_client_vpn_endpoint",
		},
		{
			Factory:  DataSourceCoIPPool,
			TypeName: "aws_ec2_coip_pool",
		},
		{
			Factory:  DataSourceCoIPPools,
			TypeName: "aws_ec2_coip_pools",
		},
		{
			Factory:  DataSourceHost,
			TypeName: "aws_ec2_host",
		},
		{
			Factory:  DataSourceInstanceType,
			TypeName: "aws_ec2_instance_type",
		},
		{
			Factory:  DataSourceInstanceTypeOffering,
			TypeName: "aws_ec2_instance_type_offering",
		},
		{
			Factory:  DataSourceInstanceTypeOfferings,
			TypeName: "aws_ec2_instance_type_offerings",
		},
		{
			Factory:  DataSourceInstanceTypes,
			TypeName: "aws_ec2_instance_types",
		},
		{
			Factory:  DataSourceLocalGateway,
			TypeName: "aws_ec2_local_gateway",
		},
		{
			Factory:  DataSourceLocalGatewayRouteTable,
			TypeName: "aws_ec2_local_gateway_route_table",
		},
		{
			Factory:  DataSourceLocalGatewayRouteTables,
			TypeName: "aws_ec2_local_gateway_route_tables",
		},
		{
			Factory:  DataSourceLocalGatewayVirtualInterface,
			TypeName: "aws_ec2_local_gateway_virtual_interface",
		},
		{
			Factory:  DataSourceLocalGatewayVirtualInterfaceGroup,
			TypeName: "aws_ec2_local_gateway_virtual_interface_group",
		},
		{
			Factory:  DataSourceLocalGatewayVirtualInterfaceGroups,
			TypeName: "aws_ec2_local_gateway_virtual_interface_groups",
		},
		{
			Factory:  DataSourceLocalGateways,
			TypeName: "aws_ec2_local_gateways",
		},
		{
			Factory:  DataSourceManagedPrefixList,
			TypeName: "aws_ec2_managed_prefix_list",
		},
		{
			Factory:  DataSourceManagedPrefixLists,
			TypeName: "aws_ec2_managed_prefix_lists",
		},
		{
			Factory:  DataSourceNetworkInsightsAnalysis,
			TypeName: "aws_ec2_network_insights_analysis",
		},
		{
			Factory:  DataSourceNetworkInsightsPath,
			TypeName: "aws_ec2_network_insights_path",
		},
		{
			Factory:  DataSourcePublicIPv4Pool,
			TypeName: "aws_ec2_public_ipv4_pool",
		},
		{
			Factory:  DataSourcePublicIPv4Pools,
			TypeName: "aws_ec2_public_ipv4_pools",
		},
		{
			Factory:  DataSourceSerialConsoleAccess,
			TypeName: "aws_ec2_serial_console_access",
		},
		{
			Factory:  DataSourceSpotPrice,
			TypeName: "aws_ec2_spot_price",
		},
		{
			Factory:  DataSourceTransitGateway,
			TypeName: "aws_ec2_transit_gateway",
		},
		{
			Factory:  DataSourceTransitGatewayAttachment,
			TypeName: "aws_ec2_transit_gateway_attachment",
		},
		{
			Factory:  DataSourceTransitGatewayConnect,
			TypeName: "aws_ec2_transit_gateway_connect",
		},
		{
			Factory:  DataSourceTransitGatewayConnectPeer,
			TypeName: "aws_ec2_transit_gateway_connect_peer",
		},
		{
			Factory:  DataSourceTransitGatewayDxGatewayAttachment,
			TypeName: "aws_ec2_transit_gateway_dx_gateway_attachment",
		},
		{
			Factory:  DataSourceTransitGatewayMulticastDomain,
			TypeName: "aws_ec2_transit_gateway_multicast_domain",
		},
		{
			Factory:  DataSourceTransitGatewayPeeringAttachment,
			TypeName: "aws_ec2_transit_gateway_peering_attachment",
		},
		{
			Factory:  DataSourceTransitGatewayRouteTable,
			TypeName: "aws_ec2_transit_gateway_route_table",
		},
		{
			Factory:  DataSourceTransitGatewayRouteTables,
			TypeName: "aws_ec2_transit_gateway_route_tables",
		},
		{
			Factory:  DataSourceTransitGatewayVPCAttachment,
			TypeName: "aws_ec2_transit_gateway_vpc_attachment",
		},
		{
			Factory:  DataSourceTransitGatewayVPCAttachments,
			TypeName: "aws_ec2_transit_gateway_vpc_attachments",
		},
		{
			Factory:  DataSourceTransitGatewayVPNAttachment,
			TypeName: "aws_ec2_transit_gateway_vpn_attachment",
		},
		{
			Factory:  DataSourceEIP,
			TypeName: "aws_eip",
		},
		{
			Factory:  DataSourceEIPs,
			TypeName: "aws_eips",
		},
		{
			Factory:  DataSourceInstance,
			TypeName: "aws_instance",
		},
		{
			Factory:  DataSourceInstances,
			TypeName: "aws_instances",
		},
		{
			Factory:  DataSourceInternetGateway,
			TypeName: "aws_internet_gateway",
		},
		{
			Factory:  DataSourceKeyPair,
			TypeName: "aws_key_pair",
		},
		{
			Factory:  DataSourceLaunchTemplate,
			TypeName: "aws_launch_template",
		},
		{
			Factory:  DataSourceNATGateway,
			TypeName: "aws_nat_gateway",
		},
		{
			Factory:  DataSourceNATGateways,
			TypeName: "aws_nat_gateways",
		},
		{
			Factory:  DataSourceNetworkACLs,
			TypeName: "aws_network_acls",
		},
		{
			Factory:  DataSourceNetworkInterface,
			TypeName: "aws_network_interface",
		},
		{
			Factory:  DataSourceNetworkInterfaces,
			TypeName: "aws_network_interfaces",
		},
		{
			Factory:  DataSourcePrefixList,
			TypeName: "aws_prefix_list",
		},
		{
			Factory:  DataSourceRoute,
			TypeName: "aws_route",
		},
		{
			Factory:  DataSourceRouteTable,
			TypeName: "aws_route_table",
		},
		{
			Factory:  DataSourceRouteTables,
			TypeName: "aws_route_tables",
		},
		{
			Factory:  DataSourceSecurityGroup,
			TypeName: "aws_security_group",
		},
		{
			Factory:  DataSourceSecurityGroups,
			TypeName: "aws_security_groups",
		},
		{
			Factory:  DataSourceSubnet,
			TypeName: "aws_subnet",
		},
		{
			Factory:  DataSourceSubnetIDs,
			TypeName: "aws_subnet_ids",
		},
		{
			Factory:  DataSourceSubnets,
			TypeName: "aws_subnets",
		},
		{
			Factory:  DataSourceVPC,
			TypeName: "aws_vpc",
		},
		{
			Factory:  DataSourceVPCDHCPOptions,
			TypeName: "aws_vpc_dhcp_options",
		},
		{
			Factory:  DataSourceVPCEndpoint,
			TypeName: "aws_vpc_endpoint",
		},
		{
			Factory:  DataSourceVPCEndpointService,
			TypeName: "aws_vpc_endpoint_service",
		},
		{
			Factory:  DataSourceIPAMPool,
			TypeName: "aws_vpc_ipam_pool",
		},
		{
			Factory:  DataSourceIPAMPoolCIDRs,
			TypeName: "aws_vpc_ipam_pool_cidrs",
		},
		{
			Factory:  DataSourceIPAMPools,
			TypeName: "aws_vpc_ipam_pools",
		},
		{
			Factory:  DataSourceIPAMPreviewNextCIDR,
			TypeName: "aws_vpc_ipam_preview_next_cidr",
		},
		{
			Factory:  DataSourceVPCPeeringConnection,
			TypeName: "aws_vpc_peering_connection",
		},
		{
			Factory:  DataSourceVPCPeeringConnections,
			TypeName: "aws_vpc_peering_connections",
		},
		{
			Factory:  DataSourceVPCs,
			TypeName: "aws_vpcs",
		},
		{
			Factory:  DataSourceVPNGateway,
			TypeName: "aws_vpn_gateway",
		},
	}
}

func (p *servicePackage) SDKResources(ctx context.Context) []*types.ServicePackageSDKResource {
	return []*types.ServicePackageSDKResource{
		{
			Factory:  ResourceAMI,
			TypeName: "aws_ami",
		},
		{
			Factory:  ResourceAMICopy,
			TypeName: "aws_ami_copy",
		},
		{
			Factory:  ResourceAMIFromInstance,
			TypeName: "aws_ami_from_instance",
		},
		{
			Factory:  ResourceAMILaunchPermission,
			TypeName: "aws_ami_launch_permission",
		},
		{
			Factory:  ResourceCustomerGateway,
			TypeName: "aws_customer_gateway",
		},
		{
			Factory:  ResourceDefaultNetworkACL,
			TypeName: "aws_default_network_acl",
		},
		{
			Factory:  ResourceDefaultRouteTable,
			TypeName: "aws_default_route_table",
		},
		{
			Factory:  ResourceDefaultSecurityGroup,
			TypeName: "aws_default_security_group",
		},
		{
			Factory:  ResourceDefaultSubnet,
			TypeName: "aws_default_subnet",
		},
		{
			Factory:  ResourceDefaultVPC,
			TypeName: "aws_default_vpc",
		},
		{
			Factory:  ResourceDefaultVPCDHCPOptions,
			TypeName: "aws_default_vpc_dhcp_options",
		},
		{
			Factory:  ResourceEBSDefaultKMSKey,
			TypeName: "aws_ebs_default_kms_key",
		},
		{
			Factory:  ResourceEBSEncryptionByDefault,
			TypeName: "aws_ebs_encryption_by_default",
		},
		{
			Factory:  ResourceEBSSnapshot,
			TypeName: "aws_ebs_snapshot",
		},
		{
			Factory:  ResourceEBSSnapshotCopy,
			TypeName: "aws_ebs_snapshot_copy",
		},
		{
			Factory:  ResourceEBSSnapshotImport,
			TypeName: "aws_ebs_snapshot_import",
		},
		{
			Factory:  ResourceEBSVolume,
			TypeName: "aws_ebs_volume",
		},
		{
			Factory:  ResourceAvailabilityZoneGroup,
			TypeName: "aws_ec2_availability_zone_group",
		},
		{
			Factory:  ResourceCapacityReservation,
			TypeName: "aws_ec2_capacity_reservation",
		},
		{
			Factory:  ResourceCarrierGateway,
			TypeName: "aws_ec2_carrier_gateway",
		},
		{
			Factory:  ResourceClientVPNAuthorizationRule,
			TypeName: "aws_ec2_client_vpn_authorization_rule",
		},
		{
			Factory:  ResourceClientVPNEndpoint,
			TypeName: "aws_ec2_client_vpn_endpoint",
		},
		{
			Factory:  ResourceClientVPNNetworkAssociation,
			TypeName: "aws_ec2_client_vpn_network_association",
		},
		{
			Factory:  ResourceClientVPNRoute,
			TypeName: "aws_ec2_client_vpn_route",
		},
		{
			Factory:  ResourceFleet,
			TypeName: "aws_ec2_fleet",
		},
		{
			Factory:  ResourceHost,
			TypeName: "aws_ec2_host",
		},
		{
			Factory:  ResourceInstanceState,
			TypeName: "aws_ec2_instance_state",
		},
		{
			Factory:  ResourceLocalGatewayRoute,
			TypeName: "aws_ec2_local_gateway_route",
		},
		{
			Factory:  ResourceLocalGatewayRouteTableVPCAssociation,
			TypeName: "aws_ec2_local_gateway_route_table_vpc_association",
		},
		{
			Factory:  ResourceManagedPrefixList,
			TypeName: "aws_ec2_managed_prefix_list",
		},
		{
			Factory:  ResourceManagedPrefixListEntry,
			TypeName: "aws_ec2_managed_prefix_list_entry",
		},
		{
			Factory:  ResourceNetworkInsightsAnalysis,
			TypeName: "aws_ec2_network_insights_analysis",
		},
		{
			Factory:  ResourceNetworkInsightsPath,
			TypeName: "aws_ec2_network_insights_path",
		},
		{
			Factory:  ResourceSerialConsoleAccess,
			TypeName: "aws_ec2_serial_console_access",
		},
		{
			Factory:  ResourceSubnetCIDRReservation,
			TypeName: "aws_ec2_subnet_cidr_reservation",
		},
		{
			Factory:  ResourceTag,
			TypeName: "aws_ec2_tag",
		},
		{
			Factory:  ResourceTrafficMirrorFilter,
			TypeName: "aws_ec2_traffic_mirror_filter",
		},
		{
			Factory:  ResourceTrafficMirrorFilterRule,
			TypeName: "aws_ec2_traffic_mirror_filter_rule",
		},
		{
			Factory:  ResourceTrafficMirrorSession,
			TypeName: "aws_ec2_traffic_mirror_session",
		},
		{
			Factory:  ResourceTrafficMirrorTarget,
			TypeName: "aws_ec2_traffic_mirror_target",
		},
		{
			Factory:  ResourceTransitGateway,
			TypeName: "aws_ec2_transit_gateway",
		},
		{
			Factory:  ResourceTransitGatewayConnect,
			TypeName: "aws_ec2_transit_gateway_connect",
		},
		{
			Factory:  ResourceTransitGatewayConnectPeer,
			TypeName: "aws_ec2_transit_gateway_connect_peer",
		},
		{
			Factory:  ResourceTransitGatewayMulticastDomain,
			TypeName: "aws_ec2_transit_gateway_multicast_domain",
		},
		{
			Factory:  ResourceTransitGatewayMulticastDomainAssociation,
			TypeName: "aws_ec2_transit_gateway_multicast_domain_association",
		},
		{
			Factory:  ResourceTransitGatewayMulticastGroupMember,
			TypeName: "aws_ec2_transit_gateway_multicast_group_member",
		},
		{
			Factory:  ResourceTransitGatewayMulticastGroupSource,
			TypeName: "aws_ec2_transit_gateway_multicast_group_source",
		},
		{
			Factory:  ResourceTransitGatewayPeeringAttachment,
			TypeName: "aws_ec2_transit_gateway_peering_attachment",
		},
		{
			Factory:  ResourceTransitGatewayPeeringAttachmentAccepter,
			TypeName: "aws_ec2_transit_gateway_peering_attachment_accepter",
		},
		{
			Factory:  ResourceTransitGatewayPolicyTable,
			TypeName: "aws_ec2_transit_gateway_policy_table",
		},
		{
			Factory:  ResourceTransitGatewayPolicyTableAssociation,
			TypeName: "aws_ec2_transit_gateway_policy_table_association",
		},
		{
			Factory:  ResourceTransitGatewayPrefixListReference,
			TypeName: "aws_ec2_transit_gateway_prefix_list_reference",
		},
		{
			Factory:  ResourceTransitGatewayRoute,
			TypeName: "aws_ec2_transit_gateway_route",
		},
		{
			Factory:  ResourceTransitGatewayRouteTable,
			TypeName: "aws_ec2_transit_gateway_route_table",
		},
		{
			Factory:  ResourceTransitGatewayRouteTableAssociation,
			TypeName: "aws_ec2_transit_gateway_route_table_association",
		},
		{
			Factory:  ResourceTransitGatewayRouteTablePropagation,
			TypeName: "aws_ec2_transit_gateway_route_table_propagation",
		},
		{
			Factory:  ResourceTransitGatewayVPCAttachment,
			TypeName: "aws_ec2_transit_gateway_vpc_attachment",
		},
		{
			Factory:  ResourceTransitGatewayVPCAttachmentAccepter,
			TypeName: "aws_ec2_transit_gateway_vpc_attachment_accepter",
		},
		{
			Factory:  ResourceEgressOnlyInternetGateway,
			TypeName: "aws_egress_only_internet_gateway",
		},
		{
			Factory:  ResourceEIP,
			TypeName: "aws_eip",
		},
		{
			Factory:  ResourceEIPAssociation,
			TypeName: "aws_eip_association",
		},
		{
			Factory:  ResourceFlowLog,
			TypeName: "aws_flow_log",
		},
		{
			Factory:  ResourceInstance,
			TypeName: "aws_instance",
		},
		{
			Factory:  ResourceInternetGateway,
			TypeName: "aws_internet_gateway",
		},
		{
			Factory:  ResourceInternetGatewayAttachment,
			TypeName: "aws_internet_gateway_attachment",
		},
		{
			Factory:  ResourceKeyPair,
			TypeName: "aws_key_pair",
		},
		{
			Factory:  ResourceLaunchTemplate,
			TypeName: "aws_launch_template",
		},
		{
			Factory:  ResourceMainRouteTableAssociation,
			TypeName: "aws_main_route_table_association",
		},
		{
			Factory:  ResourceNATGateway,
			TypeName: "aws_nat_gateway",
		},
		{
			Factory:  ResourceNetworkACL,
			TypeName: "aws_network_acl",
		},
		{
			Factory:  ResourceNetworkACLAssociation,
			TypeName: "aws_network_acl_association",
		},
		{
			Factory:  ResourceNetworkACLRule,
			TypeName: "aws_network_acl_rule",
		},
		{
			Factory:  ResourceNetworkInterface,
			TypeName: "aws_network_interface",
		},
		{
			Factory:  ResourceNetworkInterfaceAttachment,
			TypeName: "aws_network_interface_attachment",
		},
		{
			Factory:  ResourceNetworkInterfaceSGAttachment,
			TypeName: "aws_network_interface_sg_attachment",
		},
		{
			Factory:  ResourcePlacementGroup,
			TypeName: "aws_placement_group",
		},
		{
			Factory:  ResourceRoute,
			TypeName: "aws_route",
		},
		{
			Factory:  ResourceRouteTable,
			TypeName: "aws_route_table",
		},
		{
			Factory:  ResourceRouteTableAssociation,
			TypeName: "aws_route_table_association",
		},
		{
			Factory:  ResourceSecurityGroup,
			TypeName: "aws_security_group",
		},
		{
			Factory:  ResourceSecurityGroupRule,
			TypeName: "aws_security_group_rule",
		},
		{
			Factory:  ResourceSnapshotCreateVolumePermission,
			TypeName: "aws_snapshot_create_volume_permission",
		},
		{
			Factory:  ResourceSpotDataFeedSubscription,
			TypeName: "aws_spot_datafeed_subscription",
		},
		{
			Factory:  ResourceSpotFleetRequest,
			TypeName: "aws_spot_fleet_request",
		},
		{
			Factory:  ResourceSpotInstanceRequest,
			TypeName: "aws_spot_instance_request",
		},
		{
			Factory:  ResourceSubnet,
			TypeName: "aws_subnet",
		},
		{
			Factory:  ResourceVolumeAttachment,
			TypeName: "aws_volume_attachment",
		},
		{
			Factory:  ResourceVPC,
			TypeName: "aws_vpc",
		},
		{
			Factory:  ResourceVPCDHCPOptions,
			TypeName: "aws_vpc_dhcp_options",
		},
		{
			Factory:  ResourceVPCDHCPOptionsAssociation,
			TypeName: "aws_vpc_dhcp_options_association",
		},
		{
			Factory:  ResourceVPCEndpoint,
			TypeName: "aws_vpc_endpoint",
		},
		{
			Factory:  ResourceVPCEndpointConnectionAccepter,
			TypeName: "aws_vpc_endpoint_connection_accepter",
		},
		{
			Factory:  ResourceVPCEndpointConnectionNotification,
			TypeName: "aws_vpc_endpoint_connection_notification",
		},
		{
			Factory:  ResourceVPCEndpointPolicy,
			TypeName: "aws_vpc_endpoint_policy",
		},
		{
			Factory:  ResourceVPCEndpointRouteTableAssociation,
			TypeName: "aws_vpc_endpoint_route_table_association",
		},
		{
			Factory:  ResourceVPCEndpointSecurityGroupAssociation,
			TypeName: "aws_vpc_endpoint_security_group_association",
		},
		{
			Factory:  ResourceVPCEndpointService,
			TypeName: "aws_vpc_endpoint_service",
		},
		{
			Factory:  ResourceVPCEndpointServiceAllowedPrincipal,
			TypeName: "aws_vpc_endpoint_service_allowed_principal",
		},
		{
			Factory:  ResourceVPCEndpointSubnetAssociation,
			TypeName: "aws_vpc_endpoint_subnet_association",
		},
		{
			Factory:  ResourceIPAM,
			TypeName: "aws_vpc_ipam",
		},
		{
			Factory:  ResourceIPAMOrganizationAdminAccount,
			TypeName: "aws_vpc_ipam_organization_admin_account",
		},
		{
			Factory:  ResourceIPAMPool,
			TypeName: "aws_vpc_ipam_pool",
		},
		{
			Factory:  ResourceIPAMPoolCIDR,
			TypeName: "aws_vpc_ipam_pool_cidr",
		},
		{
			Factory:  ResourceIPAMPoolCIDRAllocation,
			TypeName: "aws_vpc_ipam_pool_cidr_allocation",
		},
		{
			Factory:  ResourceIPAMPreviewNextCIDR,
			TypeName: "aws_vpc_ipam_preview_next_cidr",
		},
		{
			Factory:  ResourceIPAMResourceDiscovery,
			TypeName: "aws_vpc_ipam_resource_discovery",
		},
		{
			Factory:  ResourceIPAMResourceDiscoveryAssociation,
			TypeName: "aws_vpc_ipam_resource_discovery_association",
		},
		{
			Factory:  ResourceIPAMScope,
			TypeName: "aws_vpc_ipam_scope",
		},
		{
			Factory:  ResourceVPCIPv4CIDRBlockAssociation,
			TypeName: "aws_vpc_ipv4_cidr_block_association",
		},
		{
			Factory:  ResourceVPCIPv6CIDRBlockAssociation,
			TypeName: "aws_vpc_ipv6_cidr_block_association",
		},
		{
			Factory:  ResourceNetworkPerformanceMetricSubscription,
			TypeName: "aws_vpc_network_performance_metric_subscription",
		},
		{
			Factory:  ResourceVPCPeeringConnection,
			TypeName: "aws_vpc_peering_connection",
		},
		{
			Factory:  ResourceVPCPeeringConnectionAccepter,
			TypeName: "aws_vpc_peering_connection_accepter",
		},
		{
			Factory:  ResourceVPCPeeringConnectionOptions,
			TypeName: "aws_vpc_peering_connection_options",
		},
		{
			Factory:  ResourceVPNConnection,
			TypeName: "aws_vpn_connection",
		},
		{
			Factory:  ResourceVPNConnectionRoute,
			TypeName: "aws_vpn_connection_route",
		},
		{
			Factory:  ResourceVPNGateway,
			TypeName: "aws_vpn_gateway",
		},
		{
			Factory:  ResourceVPNGatewayAttachment,
			TypeName: "aws_vpn_gateway_attachment",
		},
		{
			Factory:  ResourceVPNGatewayRoutePropagation,
			TypeName: "aws_vpn_gateway_route_propagation",
		},
	}
}

func (p *servicePackage) ServicePackageName() string {
	return names.EC2
}

var ServicePackage = &servicePackage{}