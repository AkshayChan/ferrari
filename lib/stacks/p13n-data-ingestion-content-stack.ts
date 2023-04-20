/*
 * © 2022 Amazon Web Services, Inc. or its affiliates. All Rights Reserved.
 * This AWS Content is provided subject to the terms of the AWS Customer Agreement
 * available at http://aws.amazon.com/agreement or other written agreement between Customer
 * and either Amazon Web Services, Inc. or Amazon Web Services EMEA SARL or both
 */

import * as cdk from '@aws-cdk/core';
import * as dynamodb from '@aws-cdk/aws-dynamodb';
import * as lambda from '@aws-cdk/aws-lambda';
import * as lambdapython from '@aws-cdk/aws-lambda-python';
import * as secretsmanager from '@aws-cdk/aws-secretsmanager';
import * as iam from '@aws-cdk/aws-iam';
import { Rule, Schedule } from '@aws-cdk/aws-events';
import * as events_targets from '@aws-cdk/aws-events-targets';
import { env } from '../utils/env-variables';
import { thronEnvConfig, thronEnvs, cmsEnvConfig, cmsEnvs } from '../utils/env-config';

export interface DataIngestionContentProps extends cdk.NestedStackProps {
  readonly fanAppContentDdbTableName: string;
  readonly lambdaCommonLayer: lambdapython.PythonLayerVersion;
}

export class DataIngestionContentStack extends cdk.NestedStack {
  constructor(scope: cdk.Stack, id: string, props: DataIngestionContentProps) {
    super(scope, id, props);

    const THRON_CONFIG: thronEnvConfig = thronEnvs[env.THRON_ENV];
    const CMS_CONFIG: cmsEnvConfig = cmsEnvs[env.CMS_ENV];

    const fanAppContentTable = dynamodb.Table.fromTableAttributes(
      this,
      props.fanAppContentDdbTableName,
      {
        tableName: `${props.fanAppContentDdbTableName}`,
      },
    );

    // Defines the incremental update function for Thron content
    const fanAppThronIncrementalDataLoadFunction = new lambda.Function(
      this,
      'fanAppThronIncrementalFunction',
      {
        runtime: lambda.Runtime.PYTHON_3_9, // execution environment
        code: lambda.Code.fromAsset('lib/functions/fan-app-thron'),
        handler: 'fan-app-thron-incremental.handler',
        tracing: lambda.Tracing.ACTIVE,
        timeout: cdk.Duration.seconds(900),
        memorySize: 1024,
        functionName: `${env.P13N}-thron-incremental-data-load-${env.STAGE}`,
        layers: [props.lambdaCommonLayer],
        environment: {
          CONTENT_TABLE: fanAppContentTable.tableName,
          THRON_CONFIG_SECRET_ARN: THRON_CONFIG.thronConfigSecretArn,
          THRON_ADMIN_HOST: THRON_CONFIG.thronAdminHost,
          THRON_HOST: THRON_CONFIG.thronHost,
          THRON_PUBLIC_FOLDER: THRON_CONFIG.thronPublicFolder,
          STAGE: env.STAGE,
          ENVIRONMENT_NAME: env.ENVIRONMENT_NAME,
          DAYS_AGO: '1', //IF GREATER THAN 60 IT WILL BE SET TO 60
        },
      },
    );

    /* Granting  Access to DynamoDB Table */
    fanAppContentTable.grantReadWriteData(fanAppThronIncrementalDataLoadFunction);

    /* Granting Lmabda Access  keys and Secrets */
    fanAppThronIncrementalDataLoadFunction.role?.attachInlinePolicy(
      new iam.Policy(this, 'allow-kms-decrypt', {
        statements: [
          new iam.PolicyStatement({
            actions: ['kms:Decrypt'],
            resources: ['*'],
          }),
        ],
      }),
    );

    const thronConfigSecret = secretsmanager.Secret.fromSecretCompleteArn(
      this,
      'thron-config',
      THRON_CONFIG.thronConfigSecretArn,
    );

    thronConfigSecret.grantRead(fanAppThronIncrementalDataLoadFunction);

    const ThronLambdaTarget = new events_targets.LambdaFunction(
      fanAppThronIncrementalDataLoadFunction,
      {
        retryAttempts: 2, // Optional: set the max number of retry attempts
      },
    );
    new Rule(this, 'fanAppThronIncrementalDataLoadTrigger', {
      schedule: Schedule.cron({ minute: '0', hour: '*/6', weekDay: '*' }), // every 6 hours
      targets: [ThronLambdaTarget],
    });

    // Defines the incremental update function for cms news contents
    const fanAppCMSNewsIncrementalDataLoadFunction = new lambda.Function(
      this,
      'fanAppCmsUpdateNewsDasAgoFunction',
      {
        runtime: lambda.Runtime.PYTHON_3_9, // execution environment
        code: lambda.Code.fromAsset('lib/functions/fan-app-cms'),
        handler: 'fan-app-cms-news.handler',
        tracing: lambda.Tracing.ACTIVE,
        timeout: cdk.Duration.seconds(600),
        memorySize: 1024,
        functionName: `${env.P13N}-cms-news-incremental-data-load-${env.STAGE}`,
        layers: [props.lambdaCommonLayer],
        environment: {
          CONTENT_TABLE: fanAppContentTable.tableName,
          CMS_API_KEY: env.CMS_API_KEY,
          CMS_ENDPOINT: CMS_CONFIG.cmsEndpoint,
          CMS_BASE_PATH: CMS_CONFIG.cmsBasePath,
          CDN_HOST: CMS_CONFIG.cdnHost,
          STAGE: env.STAGE,
          ENVIRONMENT_NAME: env.ENVIRONMENT_NAME,
          DAYS_AGO: '1',
        },
      },
    );

    fanAppContentTable.grantReadWriteData(fanAppCMSNewsIncrementalDataLoadFunction);

    const CmsLambdaTarget = new events_targets.LambdaFunction(
      fanAppCMSNewsIncrementalDataLoadFunction,
      {
        retryAttempts: 2, // Optional: set the max number of retry attempts
      },
    );
    new Rule(this, 'fanAppCMSNewsIncrementalDataLoadTrigger', {
      schedule: Schedule.cron({ minute: '0', hour: '*/6', weekDay: '*' }), // every 6 hours
      targets: [CmsLambdaTarget],
    });
  }
}
