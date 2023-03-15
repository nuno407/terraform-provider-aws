package apigateway

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/aws/aws-sdk-go/service/apigateway"
	"github.com/hashicorp/aws-sdk-go-base/v2/awsv1shim/v2/tfawserr"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"github.com/hashicorp/terraform-provider-aws/internal/conns"
	"github.com/hashicorp/terraform-provider-aws/internal/errs/sdkdiag"
	tftags "github.com/hashicorp/terraform-provider-aws/internal/tags"
	"github.com/hashicorp/terraform-provider-aws/internal/tfresource"
	"github.com/hashicorp/terraform-provider-aws/internal/verify"
)

// @SDKResource("aws_api_gateway_domain_name")
func ResourceDomainName() *schema.Resource {
	return &schema.Resource{
		CreateWithoutTimeout: resourceDomainNameCreate,
		ReadWithoutTimeout:   resourceDomainNameRead,
		UpdateWithoutTimeout: resourceDomainNameUpdate,
		DeleteWithoutTimeout: resourceDomainNameDelete,

		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},

		Schema: map[string]*schema.Schema{
			"arn": {
				Type:     schema.TypeString,
				Computed: true,
			},
			//According to AWS Documentation, ACM will be the only way to add certificates
			//to ApiGateway DomainNames. When this happens, we will be deprecating all certificate methods
			//except certificate_arn. We are not quite sure when this will happen.
			"certificate_arn": {
				Type:          schema.TypeString,
				Optional:      true,
				ConflictsWith: []string{"certificate_body", "certificate_chain", "certificate_name", "certificate_private_key", "regional_certificate_arn", "regional_certificate_name"},
			},
			"certificate_body": {
				Type:          schema.TypeString,
				ForceNew:      true,
				Optional:      true,
				ConflictsWith: []string{"certificate_arn", "regional_certificate_arn"},
			},
			"certificate_chain": {
				Type:          schema.TypeString,
				ForceNew:      true,
				Optional:      true,
				ConflictsWith: []string{"certificate_arn", "regional_certificate_arn"},
			},
			"certificate_name": {
				Type:          schema.TypeString,
				Optional:      true,
				ConflictsWith: []string{"certificate_arn", "regional_certificate_arn", "regional_certificate_name"},
			},
			"certificate_private_key": {
				Type:          schema.TypeString,
				ForceNew:      true,
				Optional:      true,
				Sensitive:     true,
				ConflictsWith: []string{"certificate_arn", "regional_certificate_arn"},
			},
			"certificate_upload_date": {
				Type:     schema.TypeString,
				Computed: true,
			},
			"cloudfront_domain_name": {
				Type:     schema.TypeString,
				Computed: true,
			},
			"cloudfront_zone_id": {
				Type:     schema.TypeString,
				Computed: true,
			},
			"domain_name": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"endpoint_configuration": {
				Type:     schema.TypeList,
				Optional: true,
				Computed: true,
				MinItems: 1,
				MaxItems: 1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"types": {
							Type:     schema.TypeList,
							Required: true,
							MinItems: 1,
							// BadRequestException: Cannot create an api with multiple Endpoint Types
							MaxItems: 1,
							Elem: &schema.Schema{
								Type: schema.TypeString,
								ValidateFunc: validation.StringInSlice([]string{
									apigateway.EndpointTypeEdge,
									apigateway.EndpointTypeRegional,
								}, false),
							},
						},
					},
				},
			},
			"mutual_tls_authentication": {
				Type:     schema.TypeList,
				Optional: true,
				MaxItems: 1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"truststore_uri": {
							Type:     schema.TypeString,
							Required: true,
							ForceNew: true,
						},
						"truststore_version": {
							Type:     schema.TypeString,
							Optional: true,
						},
					},
				},
			},
			"ownership_verification_certificate_arn": {
				Type:         schema.TypeString,
				Optional:     true,
				Computed:     true,
				ValidateFunc: verify.ValidARN,
			},
			"regional_certificate_arn": {
				Type:          schema.TypeString,
				Optional:      true,
				ConflictsWith: []string{"certificate_arn", "certificate_body", "certificate_chain", "certificate_name", "certificate_private_key", "regional_certificate_name"},
			},
			"regional_certificate_name": {
				Type:          schema.TypeString,
				Optional:      true,
				ConflictsWith: []string{"certificate_arn", "certificate_name", "regional_certificate_arn"},
			},
			"regional_domain_name": {
				Type:     schema.TypeString,
				Computed: true,
			},
			"regional_zone_id": {
				Type:     schema.TypeString,
				Computed: true,
			},
			"security_policy": {
				Type:         schema.TypeString,
				Optional:     true,
				Computed:     true,
				ValidateFunc: validation.StringInSlice(apigateway.SecurityPolicy_Values(), true),
			},
			"tags":     tftags.TagsSchema(),
			"tags_all": tftags.TagsSchemaComputed(),
		},

		CustomizeDiff: verify.SetTagsDiff,
	}
}

func resourceDomainNameCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	var diags diag.Diagnostics
	conn := meta.(*conns.AWSClient).APIGatewayConn()
	defaultTagsConfig := meta.(*conns.AWSClient).DefaultTagsConfig
	tags := defaultTagsConfig.MergeTags(tftags.New(ctx, d.Get("tags").(map[string]interface{})))

	domainName := d.Get("domain_name").(string)
	input := &apigateway.CreateDomainNameInput{
		DomainName:              aws.String(domainName),
		MutualTlsAuthentication: expandMutualTLSAuthentication(d.Get("mutual_tls_authentication").([]interface{})),
	}

	if v, ok := d.GetOk("certificate_arn"); ok {
		input.CertificateArn = aws.String(v.(string))
	}

	if v, ok := d.GetOk("certificate_body"); ok {
		input.CertificateBody = aws.String(v.(string))
	}

	if v, ok := d.GetOk("certificate_chain"); ok {
		input.CertificateChain = aws.String(v.(string))
	}

	if v, ok := d.GetOk("certificate_name"); ok {
		input.CertificateName = aws.String(v.(string))
	}

	if v, ok := d.GetOk("certificate_private_key"); ok {
		input.CertificatePrivateKey = aws.String(v.(string))
	}

	if v, ok := d.GetOk("endpoint_configuration"); ok {
		input.EndpointConfiguration = expandEndpointConfiguration(v.([]interface{}))
	}

	if v, ok := d.GetOk("ownership_verification_certificate_arn"); ok {
		input.OwnershipVerificationCertificateArn = aws.String(v.(string))
	}

	if v, ok := d.GetOk("regional_certificate_arn"); ok {
		input.RegionalCertificateArn = aws.String(v.(string))
	}

	if v, ok := d.GetOk("regional_certificate_name"); ok {
		input.RegionalCertificateName = aws.String(v.(string))
	}

	if v, ok := d.GetOk("security_policy"); ok {
		input.SecurityPolicy = aws.String(v.(string))
	}

	if len(tags) > 0 {
		input.Tags = Tags(tags.IgnoreAWS())
	}

	output, err := conn.CreateDomainNameWithContext(ctx, input)

	if err != nil {
		return sdkdiag.AppendErrorf(diags, "creating API Gateway Domain Name (%s): %s", domainName, err)
	}

	d.SetId(aws.StringValue(output.DomainName))

	return append(diags, resourceDomainNameRead(ctx, d, meta)...)
}

func resourceDomainNameRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	var diags diag.Diagnostics
	conn := meta.(*conns.AWSClient).APIGatewayConn()
	defaultTagsConfig := meta.(*conns.AWSClient).DefaultTagsConfig
	ignoreTagsConfig := meta.(*conns.AWSClient).IgnoreTagsConfig

	domainName, err := FindDomainName(ctx, conn, d.Id())

	if !d.IsNewResource() && tfresource.NotFound(err) {
		log.Printf("[WARN] API Gateway Domain Name (%s) not found, removing from state", d.Id())
		d.SetId("")
		return diags
	}

	if err != nil {
		return sdkdiag.AppendErrorf(diags, "reading API Gateway Domain Name (%s): %s", d.Id(), err)
	}

	arn := arn.ARN{
		Partition: meta.(*conns.AWSClient).Partition,
		Service:   "apigateway",
		Region:    meta.(*conns.AWSClient).Region,
		Resource:  fmt.Sprintf("/domainnames/%s", d.Id()),
	}.String()
	d.Set("arn", arn)
	d.Set("certificate_arn", domainName.CertificateArn)
	d.Set("certificate_name", domainName.CertificateName)
	if domainName.CertificateUploadDate != nil {
		d.Set("certificate_upload_date", domainName.CertificateUploadDate.Format(time.RFC3339))
	} else {
		d.Set("certificate_upload_date", nil)
	}
	d.Set("cloudfront_domain_name", domainName.DistributionDomainName)
	d.Set("cloudfront_zone_id", meta.(*conns.AWSClient).CloudFrontDistributionHostedZoneID())
	d.Set("domain_name", domainName.DomainName)
	if err := d.Set("endpoint_configuration", flattenEndpointConfiguration(domainName.EndpointConfiguration)); err != nil {
		return sdkdiag.AppendErrorf(diags, "setting endpoint_configuration: %s", err)
	}
	if err = d.Set("mutual_tls_authentication", flattenMutualTLSAuthentication(domainName.MutualTlsAuthentication)); err != nil {
		return sdkdiag.AppendErrorf(diags, "setting mutual_tls_authentication: %s", err)
	}
	d.Set("ownership_verification_certificate_arn", domainName.OwnershipVerificationCertificateArn)
	d.Set("regional_certificate_arn", domainName.RegionalCertificateArn)
	d.Set("regional_certificate_name", domainName.RegionalCertificateName)
	d.Set("regional_domain_name", domainName.RegionalDomainName)
	d.Set("regional_zone_id", domainName.RegionalHostedZoneId)
	d.Set("security_policy", domainName.SecurityPolicy)

	tags := KeyValueTags(ctx, domainName.Tags).IgnoreAWS().IgnoreConfig(ignoreTagsConfig)

	//lintignore:AWSR002
	if err := d.Set("tags", tags.RemoveDefaultConfig(defaultTagsConfig).Map()); err != nil {
		return sdkdiag.AppendErrorf(diags, "setting tags: %s", err)
	}

	if err := d.Set("tags_all", tags.Map()); err != nil {
		return sdkdiag.AppendErrorf(diags, "setting tags_all: %s", err)
	}

	return diags
}

func resourceDomainNameUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	var diags diag.Diagnostics
	conn := meta.(*conns.AWSClient).APIGatewayConn()

	if d.HasChangesExcept("tags", "tags_all") {
		var operations []*apigateway.PatchOperation

		if d.HasChange("certificate_arn") {
			operations = append(operations, &apigateway.PatchOperation{
				Op:    aws.String(apigateway.OpReplace),
				Path:  aws.String("/certificateArn"),
				Value: aws.String(d.Get("certificate_arn").(string)),
			})
		}

		if d.HasChange("certificate_name") {
			operations = append(operations, &apigateway.PatchOperation{
				Op:    aws.String(apigateway.OpReplace),
				Path:  aws.String("/certificateName"),
				Value: aws.String(d.Get("certificate_name").(string)),
			})
		}

		if d.HasChange("endpoint_configuration.0.types") {
			// The domain name must have an endpoint type.
			// If attempting to remove the configuration, do nothing.
			if v, ok := d.GetOk("endpoint_configuration"); ok && len(v.([]interface{})) > 0 {
				m := v.([]interface{})[0].(map[string]interface{})

				operations = append(operations, &apigateway.PatchOperation{
					Op:    aws.String(apigateway.OpReplace),
					Path:  aws.String("/endpointConfiguration/types/0"),
					Value: aws.String(m["types"].([]interface{})[0].(string)),
				})
			}
		}

		if d.HasChange("mutual_tls_authentication") {
			mutTLSAuth := d.Get("mutual_tls_authentication").([]interface{})

			if len(mutTLSAuth) == 0 || mutTLSAuth[0] == nil {
				// To disable mutual TLS for a custom domain name, remove the truststore from your custom domain name.
				operations = append(operations, &apigateway.PatchOperation{
					Op:    aws.String(apigateway.OpReplace),
					Path:  aws.String("/mutualTlsAuthentication/truststoreUri"),
					Value: aws.String(""),
				})
			} else {
				operations = append(operations, &apigateway.PatchOperation{
					Op:    aws.String(apigateway.OpReplace),
					Path:  aws.String("/mutualTlsAuthentication/truststoreVersion"),
					Value: aws.String(mutTLSAuth[0].(map[string]interface{})["truststore_version"].(string)),
				})
			}
		}

		if d.HasChange("regional_certificate_arn") {
			operations = append(operations, &apigateway.PatchOperation{
				Op:    aws.String(apigateway.OpReplace),
				Path:  aws.String("/regionalCertificateArn"),
				Value: aws.String(d.Get("regional_certificate_arn").(string)),
			})
		}

		if d.HasChange("regional_certificate_name") {
			operations = append(operations, &apigateway.PatchOperation{
				Op:    aws.String(apigateway.OpReplace),
				Path:  aws.String("/regionalCertificateName"),
				Value: aws.String(d.Get("regional_certificate_name").(string)),
			})
		}

		if d.HasChange("security_policy") {
			operations = append(operations, &apigateway.PatchOperation{
				Op:    aws.String(apigateway.OpReplace),
				Path:  aws.String("/securityPolicy"),
				Value: aws.String(d.Get("security_policy").(string)),
			})
		}

		_, err := conn.UpdateDomainNameWithContext(ctx, &apigateway.UpdateDomainNameInput{
			DomainName:      aws.String(d.Id()),
			PatchOperations: operations,
		})

		if err != nil {
			return sdkdiag.AppendErrorf(diags, "updating API Gateway Domain Name (%s): %s", d.Id(), err)
		}
	}

	if d.HasChange("tags_all") {
		o, n := d.GetChange("tags_all")

		if err := UpdateTags(ctx, conn, d.Get("arn").(string), o, n); err != nil {
			return sdkdiag.AppendErrorf(diags, "updating API Gateway Domain Name (%s) tags: %s", d.Id(), err)
		}
	}

	return append(diags, resourceDomainNameRead(ctx, d, meta)...)
}

func resourceDomainNameDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	var diags diag.Diagnostics
	conn := meta.(*conns.AWSClient).APIGatewayConn()

	log.Printf("[DEBUG] Deleting API Gateway Domain Name: %s", d.Id())
	_, err := conn.DeleteDomainNameWithContext(ctx, &apigateway.DeleteDomainNameInput{
		DomainName: aws.String(d.Id()),
	})

	if tfawserr.ErrCodeEquals(err, apigateway.ErrCodeNotFoundException) {
		return diags
	}

	if err != nil {
		return sdkdiag.AppendErrorf(diags, "deleting API Gateway Domain Name (%s): %s", d.Id(), err)
	}

	return diags
}

func FindDomainName(ctx context.Context, conn *apigateway.APIGateway, domainName string) (*apigateway.DomainName, error) {
	input := &apigateway.GetDomainNameInput{
		DomainName: aws.String(domainName),
	}

	output, err := conn.GetDomainNameWithContext(ctx, input)

	if tfawserr.ErrCodeEquals(err, apigateway.ErrCodeNotFoundException) {
		return nil, &resource.NotFoundError{
			LastError:   err,
			LastRequest: input,
		}
	}

	if err != nil {
		return nil, err
	}

	if output == nil {
		return nil, tfresource.NewEmptyResultError(input)
	}

	return output, nil
}

func expandMutualTLSAuthentication(tfList []interface{}) *apigateway.MutualTlsAuthenticationInput {
	if len(tfList) == 0 || tfList[0] == nil {
		return nil
	}

	tfMap := tfList[0].(map[string]interface{})

	apiObject := &apigateway.MutualTlsAuthenticationInput{}

	if v, ok := tfMap["truststore_uri"].(string); ok && v != "" {
		apiObject.TruststoreUri = aws.String(v)
	}

	if v, ok := tfMap["truststore_version"].(string); ok && v != "" {
		apiObject.TruststoreVersion = aws.String(v)
	}

	return apiObject
}

func flattenMutualTLSAuthentication(apiObject *apigateway.MutualTlsAuthentication) []interface{} {
	if apiObject == nil {
		return nil
	}

	tfMap := map[string]interface{}{}

	if v := apiObject.TruststoreUri; v != nil {
		tfMap["truststore_uri"] = aws.StringValue(v)
	}

	if v := apiObject.TruststoreVersion; v != nil {
		tfMap["truststore_version"] = aws.StringValue(v)
	}

	return []interface{}{tfMap}
}