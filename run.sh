export ASSET_BUCKET='rearc-data-provider'
export MANIFEST_BUCKET='norbert-test-adx-publisher-coordinator-manifest-bucket'
export CUSTOMER_ID='rearc'
export DATASET_NAME='nextstrain-hcov-19-platform'
export DATASET_ARN='arn:aws:dataexchange:us-east-1:796406704065:data-sets/6afda64ca351e462006618317d6c2f7b'
export PRODUCT_NAME='Genomic Epidemiology of Novel Coronavirus (COVID-19) | Nextstrain'
export PRODUCT_ID='prod-tafw6tmtrgmza'
export SCHEDULE_CRON="cron(0 9 ? * 3 *)"
export REGION='us-east-1'
# export PROFILE='guardian-pg'

echo "AssetBucket: $ASSET_BUCKET"
echo "ManifestBucket: $MANIFEST_BUCKET"
echo "CustomerId: $CUSTOMER_ID"
echo "DataSetName: $DATASET_NAME"
echo "DataSetArn: $DATASET_ARN"
echo "ProductName: $PRODUCT_NAME"
echo "ProductID: $PRODUCT_ID"
echo "ScheduleCron: $SCHEDULE_CRON"
echo "Region: $REGION"
echo "PROFILE: $PROFILE"

# python pre-processing/pre-processing-code/source_data.py

./init.sh \
    --schedule_cron "${SCHEDULE_CRON}" \
    --asset-bucket "${ASSET_BUCKET}" \
    --manifest-bucket "${MANIFEST_BUCKET}" \
    --customer-id "${CUSTOMER_ID}" \
    --dataset-name "${DATASET_NAME}" \
    --product-name "${PRODUCT_NAME}" \
    --product-id "${PRODUCT_ID}" \
    --region "${REGION}" \
    --first_revision "false"
    # --profile "${PROFILE}"

