package aws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/opensearch"

	opensearchservicev1 "github.com/aws/aws-sdk-go/service/opensearchservice"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
)

//// TABLE DEFINITION

func tableAwsOpenSearchReservedInstance(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "aws_opensearch_reserved_instance",
		Description: "AWS OpenSearch Reserved Instance",
		Get: &plugin.GetConfig{
			KeyColumns: plugin.SingleColumn("reserved_instance_id"),
			Hydrate:    getOpenSearchReservedInstance,
		},
		List: &plugin.ListConfig{
			Hydrate: listOpenSearchReservedInstances,
			KeyColumns: []*plugin.KeyColumn{
				{Name: "reserved_instance_id", Require: plugin.Optional},
			},
		},
		GetMatrixItemFunc: SupportedRegionMatrix(opensearchservicev1.EndpointsID),
		Columns: awsRegionalColumns([]*plugin.Column{
			{
				Name:        "reserved_instance_id",
				Description: "The unique identifier for the reservation.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "reservation_name",
				Description: "The customer-specified identifier to track this reservation.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "billing_subscription_id",
				Description: "The unique identifier of the billing subscription.",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "reserved_instance_offering_id",
				Description: "The unique identifier of the Reserved Instance offering.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "instance_type",
				Description: "The OpenSearch instance type offered by theReserved Instance offering.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "start_time",
				Description: "The date and time when the reservation was purchased.",
				Type:        proto.ColumnType_TIMESTAMP,
			},
			{
				Name:        "duration",
				Description: "The duration, in seconds, for which the OpenSearch instance is reserved.",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "fixed_price",
				Description: "The upfront fixed charge you will paid to purchase the specific Reserved Instance offering.",
				Type:        proto.ColumnType_DOUBLE,
			},
			{
				Name:        "usage_price",
				Description: "The hourly rate at which you're charged for the domain using this Reserved Instance.",
				Type:        proto.ColumnType_DOUBLE,
			},
			{
				Name:        "currency_code",
				Description: "The currency code for the offering.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "instance_count",
				Description: "The number of OpenSearch instances that have been reserved.",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "state",
				Description: "The state of the Reserved Instance.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "payment_option",
				Description: "The payment option as defined in the Reserved Instance offering.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "recurring_charges",
				Description: "The recurring charge to your account, regardless of whether you create any domains using the Reserved Instance offering.",
				Type:        proto.ColumnType_JSON,
			},

			// Steampipe standard columns
			{
				Name:        "title",
				Description: resourceInterfaceDescription("title"),
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("ReservedInstanceId"),
			},
			{
				Name:        "akas",
				Description: resourceInterfaceDescription("akas"),
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("ReservedInstanceId").Transform(transform.EnsureStringArray),
			},
		}),
	}
}

//// LIST FUNCTION

func listOpenSearchReservedInstances(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	// Create Session
	svc, err := OpenSearchClient(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("aws_opensearch_reserved_instance.listOpenSearchReservedInstances", "get_client_error", err)
		return nil, err
	}

	input := &opensearch.DescribeReservedInstancesInput{
		MaxResults: *aws.Int32(100),
	}

	if d.EqualsQuals["reserved_instance_id"] != nil {
		input.ReservedInstanceId = aws.String(d.EqualsQuals["reserved_instance_id"].GetStringValue())
	}

	if d.QueryContext.Limit != nil {
		limit := int32(*d.QueryContext.Limit)
		if limit < input.MaxResults {
			if limit < 20 {
				input.MaxResults = *aws.Int32(20)
			} else {
				input.MaxResults = *aws.Int32(limit)
			}
		}
	}

	// // List call
	paginator := opensearch.NewDescribeReservedInstancesPaginator(svc, input, func(o *opensearch.DescribeReservedInstancesPaginatorOptions) {
		o.Limit = input.MaxResults
		o.StopOnDuplicateToken = true
	})

	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			plugin.Logger(ctx).Error("aws_opensearch_reserved_instance.listOpenSearchReservedInstances", "api_error", err)
			return nil, err
		}

		for _, reservedInstance := range output.ReservedInstances {
			d.StreamListItem(ctx, reservedInstance)

			// Context can be cancelled due to manual cancellation or the limit has been hit
			if d.RowsRemaining(ctx) == 0 {
				return nil, nil
			}
		}
	}

	return nil, nil
}

//// HYDRATE FUNCTIONS

func getOpenSearchReservedInstance(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	quals := d.EqualsQuals
	reservedInstanceId := quals["reserved_instance_id"].GetStringValue()

	// check if reservedInstanceId is empty
	if reservedInstanceId == "" {
		return nil, nil
	}

	// Create service
	svc, err := OpenSearchClient(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("aws_opensearch_reserved_instance.getOpenSearchReservedInstance", "get_client_error", err)
		return nil, err
	}

	params := &opensearch.DescribeReservedInstancesInput{
		ReservedInstanceId: aws.String(reservedInstanceId),
	}

	op, err := svc.DescribeReservedInstances(ctx, params)
	if err != nil {
		plugin.Logger(ctx).Error("aws_opensearch_reserved_instance.getOpenSearchReservedInstance", "api_error", err)
		return nil, err
	}

	if len(op.ReservedInstances) > 0 {
		return op.ReservedInstances[0], nil
	}
	return nil, nil
}
