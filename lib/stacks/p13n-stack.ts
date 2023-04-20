/*
 * © 2022 Amazon Web Services, Inc. or its affiliates. All Rights Reserved.
 * This AWS Content is provided subject to the terms of the AWS Customer Agreement
 * available at http://aws.amazon.com/agreement or other written agreement between Customer
 * and either Amazon Web Services, Inc. or Amazon Web Services EMEA SARL or both
 */

import * as cdk from '@aws-cdk/core';
import * as s3 from '@aws-cdk/aws-s3';
import * as iam from '@aws-cdk/aws-iam';
import * as personalize from '@aws-cdk/aws-personalize';
import * as dynamodb from '@aws-cdk/aws-dynamodb';
import * as lambdapython from '@aws-cdk/aws-lambda-python';
import * as lambda from '@aws-cdk/aws-lambda';
import { env } from '../utils/env-variables';

export class FanAppPersonalizationStack extends cdk.Stack {
  public readonly fanAppPersonalisationBucket: s3.Bucket;
  public readonly fanAppPersonalisationImportRole: iam.Role;
  public readonly fanAppPersonalisationVideoDatasetGroup: personalize.CfnDatasetGroup;
  public readonly fanAppPersonalisationNewsDatasetGroup: personalize.CfnDatasetGroup;
  public readonly fanAppContentDdbTable: dynamodb.Table;
  public readonly fanAppContentDdbTableName: string;
  public readonly commonLambdaLayer: lambdapython.PythonLayerVersion;

  constructor(scope: cdk.App, id: string, props: cdk.StackProps) {
    super(scope, id, props);

    // DynamoDB table for storing content IDs and URLs
    this.fanAppContentDdbTableName = `${env.P13N}-content-data-cache-${env.STAGE}`;
    const fanAppContentTable = new dynamodb.Table(this, 'fanAppContentTable', {
      partitionKey: { name: 'contentId', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      tableName: this.fanAppContentDdbTableName,
      stream: dynamodb.StreamViewType.NEW_IMAGE,
    });

    this.fanAppContentDdbTable = fanAppContentTable;

    // Create the S3 bucket to store raw user behaviour data
    // This buckets are not removed after stack destroy
    const fanAppPersonaliseBucket = new s3.Bucket(this, 'fanAppPersonalisationBucket', {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      bucketName: `${env.P13N}-personalize-bucket-${cdk.Aws.ACCOUNT_ID}-${env.STAGE}`,
      encryption: s3.BucketEncryption.S3_MANAGED,
    });
    this.fanAppPersonalisationBucket = fanAppPersonaliseBucket;

    // Create a bucket policy for personalize to access the data
    fanAppPersonaliseBucket.addToResourcePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        principals: [new iam.ServicePrincipal('personalize.amazonaws.com')],
        actions: ['s3:GetObject', 's3:ListBucket'],
        resources: [
          `${fanAppPersonaliseBucket.bucketArn}/*`,
          `${fanAppPersonaliseBucket.bucketArn}`,
        ],
      }),
    );

    // Create a role with permissions for Amazon Personalize to create dataset import jobs
    const fanAppPersonaliseImportRole = new iam.Role(this, 'fanAppPersonaliseImportRole', {
      assumedBy: new iam.ServicePrincipal('personalize.amazonaws.com'),
      roleName: `${env.P13N}-personalize-import-role-${env.STAGE}`,
    });

    // Grants read and write to the user behaviour data, including any encryption/decryption
    this.fanAppPersonalisationBucket.grantReadWrite(fanAppPersonaliseImportRole);
    this.fanAppPersonalisationImportRole = fanAppPersonaliseImportRole;

    // Create the videos personalize dataset group
    const fanAppPersonaliseVideoDatasetGroup = new personalize.CfnDatasetGroup(
      this,
      'fanAppPersonaliseVideoDatasetGroup',
      {
        name: `${env.P13N}-video-dataset-group-${env.STAGE}`,
      },
    );

    this.fanAppPersonalisationVideoDatasetGroup = fanAppPersonaliseVideoDatasetGroup;

    // Create the videos personalize dataset group
    const fanAppPersonaliseNewsDatasetGroup = new personalize.CfnDatasetGroup(
      this,
      'fanAppPersonaliseNewsDatasetGroup',
      {
        name: `${env.P13N}-news-dataset-group-${env.STAGE}`,
      },
    );
    this.fanAppPersonalisationNewsDatasetGroup = fanAppPersonaliseNewsDatasetGroup;

    // Create common layer for lambdas (check requirements.txt)
    const fanAppCommonLayer = new lambdapython.PythonLayerVersion(this, 'fanAppCommonLayer', {
      compatibleRuntimes: [
        lambda.Runtime.PYTHON_3_9, // execution environment
      ],
      entry: 'lib/pythonlayers/common',
      description: 'Common layer for functions',
      layerVersionName: `${env.P13N}-common-layer-${env.STAGE}`,
    });
    this.commonLambdaLayer = fanAppCommonLayer;
  }
}
