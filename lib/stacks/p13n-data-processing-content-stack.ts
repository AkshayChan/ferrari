/*
 * © 2022 Amazon Web Services, Inc. or its affiliates. All Rights Reserved.
 * This AWS Content is provided subject to the terms of the AWS Customer Agreement
 * available at http://aws.amazon.com/agreement or other written agreement between Customer
 * and either Amazon Web Services, Inc. or Amazon Web Services EMEA SARL or both
 */

import * as kms from '@aws-cdk/aws-kms';
import * as cdk from '@aws-cdk/core';
import * as s3 from '@aws-cdk/aws-s3';
import * as glue from '@aws-cdk/aws-glue';
import * as iam from '@aws-cdk/aws-iam';
import * as personalize from '@aws-cdk/aws-personalize';
import * as dynamodb from '@aws-cdk/aws-dynamodb';
import * as lambda from '@aws-cdk/aws-lambda';
import * as lambdapython from '@aws-cdk/aws-lambda-python';
import * as sfn from '@aws-cdk/aws-stepfunctions';
import { Rule, Schedule } from '@aws-cdk/aws-events';
import * as events_targets from '@aws-cdk/aws-events-targets';
import * as tasks from '@aws-cdk/aws-stepfunctions-tasks';
import * as secretsmanager from '@aws-cdk/aws-secretsmanager';
import { env } from '../utils/env-variables';
import { cmsEnvConfig, cmsEnvs, thronEnvConfig, thronEnvs } from '../utils/env-config';

export interface DataProcessingContentProps extends cdk.NestedStackProps {
  readonly fanAppPersonalisationBucket: s3.IBucket;
  readonly fanAppPersonalisationImportRole: iam.IRole;
  readonly fanAppPersonalisationVideoDatasetGroup: personalize.CfnDatasetGroup;
  readonly fanAppPersonalisationNewsDatasetGroup: personalize.CfnDatasetGroup;
  readonly fanAppContentDdbTableName: string;
  readonly fanAppContentDdbTable: dynamodb.Table;
  readonly lambdaCommonLayer: lambdapython.PythonLayerVersion;
}

export class DataProcessingContentStack extends cdk.NestedStack {
  constructor(scope: cdk.Stack, id: string, props: DataProcessingContentProps) {
    super(scope, id, props);

    /*
     * Glue jobs for interactions data
     * @author  Akshay Chandiramani
     */

    // Define a role for the glue job policy
    const userBehaviourJobRole = new iam.Role(this, 'userBehaviourJobRole', {
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
      roleName: `${env.P13N}-user-behaviour-job-role-${env.STAGE}`,
    });

    userBehaviourJobRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'),
    );

    // Give the glue job role access to Amazon Personalize
    userBehaviourJobRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AmazonPersonalizeFullAccess'),
    );

    // Give glue job access to the Personalize data bucket
    userBehaviourJobRole.addToPolicy(
      new iam.PolicyStatement({
        actions: [
          's3:Abort*',
          's3:DeleteObject*',
          's3:GetBucket*',
          's3:GetObject*',
          's3:List*',
          's3:PutObject*',
          'ssm:PutParameter',
          'ssm:GetParameters',
          'ssm:GetParameter',
        ],
        resources: [
          `arn:aws:s3:::fanapp-pinpoint-events-${env.STAGE}/*`,
          `arn:aws:s3:::fanapp-pinpoint-events-${env.STAGE}`,
          `arn:aws:ssm:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:parameter/*`,
        ],
      }),
    );

    // Grants read and write to the user behaviour data, including any encryption/decryption
    props.fanAppPersonalisationBucket.grantReadWrite(userBehaviourJobRole);

    const userBehaviourJob = new glue.Job(this, 'userBehaviourJob', {
      executable: glue.JobExecutable.pythonEtl({
        glueVersion: glue.GlueVersion.V3_0,
        pythonVersion: glue.PythonVersion.THREE,
        script: glue.Code.fromAsset('lib/jobs/fan-app-user-behaviour/main.py'),
      }),
      description: 'glue job to transform user behaviour data',
      jobName: `${env.P13N}-user-behaviour-job-${env.STAGE}`,
      role: userBehaviourJobRole,
      workerType: glue.WorkerType.G_2X,
      workerCount: 10,
      defaultArguments: {
        '--personalize_data_bucket': `fanapp-pinpoint-events-${env.STAGE}`,
        '--personalize_bucket_name': props.fanAppPersonalisationBucket.bucketName,
        '--personalize_video_dataset_group':
          props.fanAppPersonalisationVideoDatasetGroup.attrDatasetGroupArn,
        '--personalize_news_dataset_group':
          props.fanAppPersonalisationNewsDatasetGroup.attrDatasetGroupArn,
        '--personalize_import_role': props.fanAppPersonalisationImportRole.roleArn,
      },
    });

    const pinPointKey = kms.Key.fromKeyArn(this, 'Key', env.PINPOINT_KEY);
    pinPointKey.grantDecrypt(userBehaviourJob);

    // Incremental user preferences job
    const userBehaviourIncrementalJob = new glue.Job(this, 'userBehaviourIncrementalJob', {
      executable: glue.JobExecutable.pythonEtl({
        glueVersion: glue.GlueVersion.V3_0,
        pythonVersion: glue.PythonVersion.THREE,
        script: glue.Code.fromAsset('lib/jobs/fan-app-user-behaviour-incremental/main.py'),
      }),
      description: 'glue job to transform daily user behaviour data',
      jobName: `${env.P13N}-user-behaviour-incremental-job-${env.STAGE}`,
      role: userBehaviourJobRole,
      workerType: glue.WorkerType.G_2X,
      workerCount: 10,
      defaultArguments: {
        '--personalize_data_bucket': `fanapp-pinpoint-events-${env.STAGE}`,
        '--personalize_bucket_name': props.fanAppPersonalisationBucket.bucketName,
        '--personalize_video_dataset_group':
          props.fanAppPersonalisationVideoDatasetGroup.attrDatasetGroupArn,
        '--personalize_news_dataset_group':
          props.fanAppPersonalisationNewsDatasetGroup.attrDatasetGroupArn,
        '--personalize_import_role': props.fanAppPersonalisationImportRole.roleArn,
        '--additional-python-modules': 'botocore>=1.29.33,boto3>=1.26.33',
      },
    });

    // Trigger Glue job at 2am UTC everyday
    new glue.CfnTrigger(this, 'userBehaviourIncrementalJobTrigger', {
      type: 'SCHEDULED',
      description: 'Trigger to run incremental glue job every day',
      name: `${env.P13N}-user-behaviour-incremental-job-trigger-${env.STAGE}`,
      schedule: 'cron(0 2 * * ? *)',
      startOnCreation: true,
      actions: [
        {
          jobName: userBehaviourIncrementalJob.jobName,
        },
      ],
    });

    // Now will work on processing the User Preferences Data
    // Create the role for the lambda functions that process data (dynamo DB and S3)
    const lambdaProcessingRole = new iam.Role(this, 'lambdaProcessingRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
    });

    lambdaProcessingRole.addToPolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: [
          'lambda:InvokeFunction',
          'logs:CreateLogGroup',
          'logs:CreateLogStream',
          'logs:PutLogEvents',
          'dynamodb:*',
          's3:*',
          'sts:*',
          'personalize:*',
          'iam:*',
          'ssm:PutParameter',
          'ssm:GetParameters',
          'ssm:GetParameter',
        ],
        resources: ['*', `arn:aws:ssm:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:parameter/*`],
      }),
    );

    const usersProfilesTable = dynamodb.Table.fromTableAttributes(
      this,
      `fan-app-profiles-${env.STAGE}`,
      {
        // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
        tableStreamArn: `${env.DDB_PROF_STR}`,
        tableName: `fan-app-profiles-${env.STAGE}`,
      },
    );
    usersProfilesTable.grantReadWriteData(lambdaProcessingRole);

    // Create Pandas Lambda Layer
    const fanAppPandasLambdaLayer = new lambdapython.PythonLayerVersion(this, 'fanAppPandasLayer', {
      compatibleRuntimes: [
        lambda.Runtime.PYTHON_3_9, // execution environment
      ],
      entry: 'lib/pythonlayers/pandas',
      description: 'Lambda layer for pandas python library',
      layerVersionName: `${env.P13N}-pandas-layer-${env.STAGE}`,
    });

    // Create lambda layer for dynamodb-json library
    const fanAppDDBJsonLayer = new lambdapython.PythonLayerVersion(this, 'fanAppDDBJsonLayer', {
      compatibleRuntimes: [
        lambda.Runtime.PYTHON_3_9, // execution environment
      ],
      entry: 'lib/pythonlayers/ddbjson',
      description: 'Lambda layer for dynamodb-json python library',
      layerVersionName: `${env.P13N}-ddbjson-layer-${env.STAGE}`,
    });

    // Create the lambda function for the initial import of user preferences to the Personalize dataset for users
    const initUserPreferencesDataImportFunction = new lambda.Function(
      this,
      `${env.P13N}-init-users-preferences-data-${env.STAGE}`,
      {
        runtime: lambda.Runtime.PYTHON_3_9,
        code: lambda.Code.fromAsset('lib/functions/data-preparation'),
        handler: 'init_user_preferences_import.handler',
        functionName: `${env.P13N}-user-prefs-initial-data-ingestion-${env.STAGE}`,
        role: lambdaProcessingRole,
        layers: [fanAppPandasLambdaLayer],
        timeout: cdk.Duration.seconds(900),
        memorySize: 1024,
        environment: {
          P13N: env.P13N,
          STAGE: env.STAGE,
          ENVIRONMENT_NAME: env.ENVIRONMENT_NAME,
          ACCOUNT_ID: cdk.Aws.ACCOUNT_ID,
          S3_BUCKET_NAME: props.fanAppPersonalisationBucket.bucketName,
          ROLE_IMPORT: props.fanAppPersonalisationImportRole.roleArn,
          DATASET_VIDEO_GROUP_ARN: props.fanAppPersonalisationVideoDatasetGroup.attrDatasetGroupArn,
          DATASET_NEWS_GROUP_ARN: props.fanAppPersonalisationNewsDatasetGroup.attrDatasetGroupArn,
        },
      },
    );

    // Create a lambda function for the incremental imports to user preferences from the initial table to S3
    const incrementalUserPreferencesDataImportFunction = new lambda.Function(
      this,
      `${env.P13N}-incremental-users-preferences-data-${env.STAGE}`,
      {
        runtime: lambda.Runtime.PYTHON_3_9,
        code: lambda.Code.fromAsset('lib/functions/data-preparation'),
        handler: 'incremental_user_preferences_import.handler',
        functionName: `${env.P13N}-user-prefs-incremental-data-ingestion-${env.STAGE}`,
        role: lambdaProcessingRole,
        timeout: cdk.Duration.seconds(600),
        layers: [fanAppDDBJsonLayer],
        memorySize: 1024,
        environment: {
          P13N: env.P13N,
          STAGE: env.STAGE,
          ENVIRONMENT_NAME: env.ENVIRONMENT_NAME,
          DATASET_VIDEO_GROUP_ARN: props.fanAppPersonalisationVideoDatasetGroup.attrDatasetGroupArn,
          DATASET_NEWS_GROUP_ARN: props.fanAppPersonalisationNewsDatasetGroup.attrDatasetGroupArn,
        },
      },
    );

    const incrementalUserPreferencesDataImportSourceMapping =
      incrementalUserPreferencesDataImportFunction.addEventSourceMapping(
        'incrementalUserPreferencesDataImportSourceMapping',
        {
          eventSourceArn: usersProfilesTable.tableStreamArn,
          batchSize: 100,
          maxBatchingWindow: cdk.Duration.minutes(5),
          startingPosition: lambda.StartingPosition.TRIM_HORIZON,
          bisectBatchOnError: true,
          retryAttempts: 2,
        },
      );
    const cfnincrementalUserPreferencesDataImportSourceMapping =
      incrementalUserPreferencesDataImportSourceMapping.node
        .defaultChild as lambda.CfnEventSourceMapping;
    if (cfnincrementalUserPreferencesDataImportSourceMapping)
      cfnincrementalUserPreferencesDataImportSourceMapping.addPropertyOverride('FilterCriteria', {
        Filters: [
          {
            Pattern: JSON.stringify({
              dynamodb: {
                Keys: {
                  sk: { S: ['fanApp#onboarding#'] },
                },
              },
              eventName: [{ 'anything-but': ['REMOVE'] }],
            }),
          },
        ],
      });

    //import the existing table with users data not processed
    const content_table = dynamodb.Table.fromTableAttributes(
      this,
      props.fanAppContentDdbTableName,
      {
        // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
        tableStreamArn: props.fanAppContentDdbTable.tableStreamArn,
        tableName: props.fanAppContentDdbTableName,
      },
    );
    content_table.grantReadWriteData(lambdaProcessingRole);

    // Create the lambda function to fetch initial data from content DynamoDB table and ingest data to Personalize
    const contentInitialIngestionLambda = new lambda.Function(this, 'content_data_ingestion', {
      runtime: lambda.Runtime.PYTHON_3_9,
      code: lambda.Code.fromAsset('lib/functions/data-preparation'),
      handler: 'content_data_ingestion.lambda_handler',
      functionName: `${env.P13N}-content-initial-data-ingestion-${env.STAGE}`,
      role: lambdaProcessingRole,
      layers: [fanAppPandasLambdaLayer, fanAppDDBJsonLayer],
      timeout: cdk.Duration.seconds(900),
      memorySize: 1024,
      environment: {
        CONTENT_BUCKET: props.fanAppPersonalisationBucket.bucketName,
        CONTENT_TABLE: props.fanAppContentDdbTableName,
        VIDEO_GROUP_ARN: props.fanAppPersonalisationVideoDatasetGroup.attrDatasetGroupArn,
        NEWS_GROUP_ARN: props.fanAppPersonalisationNewsDatasetGroup.attrDatasetGroupArn,
        ROLE_IMPORT: props.fanAppPersonalisationImportRole.roleArn,
        P13N: env.P13N,
        STAGE: env.STAGE,
        ENVIRONMENT_NAME: env.ENVIRONMENT_NAME,
      },
    });

    // Create the lambda function to put incremental items to personalize datasets
    const contentIncrementalIngestionLambda = new lambda.Function(
      this,
      'incremental_content_data_ingestion',
      {
        runtime: lambda.Runtime.PYTHON_3_9,
        code: lambda.Code.fromAsset('lib/functions/data-preparation'),
        handler: 'incremental_content_data_ingestion.lambda_handler',
        functionName: `${env.P13N}-content-incremental-data-ingestion-${env.STAGE}`,
        role: lambdaProcessingRole,
        layers: [fanAppDDBJsonLayer],
        timeout: cdk.Duration.seconds(900),
        memorySize: 1024,
        environment: {
          P13N: env.P13N,
          STAGE: env.STAGE,
          ENVIRONMENT_NAME: env.ENVIRONMENT_NAME,
        },
      },
    );

    const incrementalContentDataImportSourceMapping =
      contentIncrementalIngestionLambda.addEventSourceMapping(
        'incrementalContentDataImportSourceMapping',
        {
          eventSourceArn: content_table.tableStreamArn,
          batchSize: 100,
          maxBatchingWindow: cdk.Duration.seconds(30),
          startingPosition: lambda.StartingPosition.TRIM_HORIZON,
          bisectBatchOnError: true,
          retryAttempts: 2,
        },
      );
    const cfnincrementalContentDataImportSourceMapping = incrementalContentDataImportSourceMapping
      .node.defaultChild as lambda.CfnEventSourceMapping;
    if (cfnincrementalContentDataImportSourceMapping)
      cfnincrementalContentDataImportSourceMapping.addPropertyOverride('FilterCriteria', {
        Filters: [
          {
            Pattern: JSON.stringify({
              eventName: [{ 'anything-but': ['REMOVE'] }],
            }),
          },
        ],
      });

    /*
     * Lambda/Step function for initial pipeline
     * @author  Akshay Chandiramani/Vincenzo Cerbone/Maria Majewska
     */

    // Define a role for the Lambda creating the solution
    const personalizeInitialSolutionRole = new iam.Role(this, 'personalizeInitialSolutionRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      roleName: `${env.P13N}-personalize-initial-solution-role-${env.STAGE}`,
    });

    personalizeInitialSolutionRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
    );

    personalizeInitialSolutionRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaVPCAccessExecutionRole'),
    );

    // Give the Lambda access to Amazon Personalize
    personalizeInitialSolutionRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AmazonPersonalizeFullAccess'),
    );

    const ssmPolicyStatement = new iam.PolicyStatement({
      actions: ['ssm:PutParameter', 'ssm:GetParameters', 'ssm:GetParameter'],
      resources: [`arn:aws:ssm:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:parameter/*`],
    });

    const ssmPolicy = new iam.Policy(this, 'ssm-policy', {
      statements: [ssmPolicyStatement],
    });

    personalizeInitialSolutionRole.attachInlinePolicy(ssmPolicy);

    // Defines the initial personalize function to create the 1st solution version
    const fanAppPersonalizeInitialSolution = new lambda.Function(
      this,
      'fanAppPersonalizeInitialSolution',
      {
        runtime: lambda.Runtime.PYTHON_3_9, // execution environment
        code: lambda.Code.fromAsset('lib/functions/fan-app-personalize'),
        handler: 'fan-app-personalize-initial-solution.handler',
        tracing: lambda.Tracing.ACTIVE,
        timeout: cdk.Duration.seconds(600),
        memorySize: 1024,
        functionName: `${env.P13N}-personalize-initial-solution-${env.STAGE}`,
        role: personalizeInitialSolutionRole,
        environment: {
          VIDEO_DATASET_GROUP: props.fanAppPersonalisationVideoDatasetGroup.attrDatasetGroupArn,
          NEWS_DATASET_GROUP: props.fanAppPersonalisationNewsDatasetGroup.attrDatasetGroupArn,
          STAGE: env.STAGE,
          ENVIRONMENT_NAME: env.ENVIRONMENT_NAME,
        },
      },
    );

    // Define a role for the Lambda creating the campaign
    const personalizeInitialCampaignRole = new iam.Role(this, 'personalizeInitialCampaignRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      roleName: `${env.P13N}-personalize-initial-campaign-role-${env.STAGE}`,
    });

    personalizeInitialCampaignRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
    );

    personalizeInitialCampaignRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaVPCAccessExecutionRole'),
    );

    // Give the Lambda access to Amazon Personalize
    personalizeInitialCampaignRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AmazonPersonalizeFullAccess'),
    );

    personalizeInitialCampaignRole.attachInlinePolicy(ssmPolicy);

    // Defines the initial personalize function to create or update the campaign
    const fanAppPersonalizeInitialCampaign = new lambda.Function(
      this,
      'fanAppPersonalizeInitialCampaign',
      {
        runtime: lambda.Runtime.PYTHON_3_9, // execution environment
        code: lambda.Code.fromAsset('lib/functions/fan-app-personalize'),
        handler: 'fan-app-personalize-initial-campaign.handler',
        tracing: lambda.Tracing.ACTIVE,
        timeout: cdk.Duration.seconds(600),
        memorySize: 1024,
        functionName: `${env.P13N}-personalize-initial-campaign-${env.STAGE}`,
        role: personalizeInitialCampaignRole,
        environment: {
          CAMPAIGN_NAME_VIDEO: `fan-appvideo-similar_items-${env.STAGE}`,
          CAMPAIGN_NAME_NEWS: `fan-appnews-similar_items-${env.STAGE}`,
          STAGE: env.STAGE,
          ENVIRONMENT_NAME: env.ENVIRONMENT_NAME,
        },
      },
    );

    // Define a role for the Lambda creating the event tracker
    const personalizeEventTrackerRole = new iam.Role(this, 'personalizeEventTrackerRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      roleName: `${env.P13N}-personalize-event-tracker-role-${env.STAGE}`,
    });

    personalizeEventTrackerRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
    );

    personalizeEventTrackerRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaVPCAccessExecutionRole'),
    );

    // Give the Lambda access to Amazon Personalize
    personalizeEventTrackerRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AmazonPersonalizeFullAccess'),
    );

    personalizeEventTrackerRole.attachInlinePolicy(ssmPolicy);

    // Defines the initial personalize function to create or update the campaign
    const fanAppPersonalizeEventTracker = new lambda.Function(
      this,
      'fanAppPersonalizeEventTracker',
      {
        runtime: lambda.Runtime.PYTHON_3_9, // execution environment
        code: lambda.Code.fromAsset('lib/functions/fan-app-personalize'),
        handler: 'fan-app-personalize-event-tracker.handler',
        tracing: lambda.Tracing.ACTIVE,
        timeout: cdk.Duration.seconds(600),
        memorySize: 1024,
        functionName: `${env.P13N}-personalize-event-tracker-${env.STAGE}`,
        role: personalizeEventTrackerRole,
        environment: {
          VIDEO_DATASET_GROUP: props.fanAppPersonalisationVideoDatasetGroup.attrDatasetGroupArn,
          NEWS_DATASET_GROUP: props.fanAppPersonalisationNewsDatasetGroup.attrDatasetGroupArn,
          STAGE: env.STAGE,
        },
      },
    );

    // INITIAL UPDATE FOR TRON CONTENT AND LOAD TO CONTENT DDB TABLE
    const CMS_CONFIG: cmsEnvConfig = cmsEnvs[env.CMS_ENV];
    const THRON_CONFIG: thronEnvConfig = thronEnvs[env.THRON_ENV];

    // Defines the initial update function for Thron content
    const fanAppThronInitialFunction = new lambda.Function(this, 'fanAppThronInitialFunction', {
      runtime: lambda.Runtime.PYTHON_3_9, // execution environment
      code: lambda.Code.fromAsset('lib/functions/fan-app-thron'),
      handler: 'fan-app-thron-initial.handler',
      tracing: lambda.Tracing.ACTIVE,
      timeout: cdk.Duration.seconds(600),
      memorySize: 1024,
      functionName: `${env.P13N}-thron-initial-data-load-${env.STAGE}`,
      layers: [props.lambdaCommonLayer],
      environment: {
        CONTENT_TABLE: content_table.tableName,
        THRON_CONFIG_SECRET_ARN: THRON_CONFIG.thronConfigSecretArn,
        THRON_ADMIN_HOST: THRON_CONFIG.thronAdminHost,
        THRON_HOST: THRON_CONFIG.thronHost,
        THRON_PUBLIC_FOLDER: THRON_CONFIG.thronPublicFolder,
        STAGE: env.STAGE,
        ENVIRONMENT_NAME: env.ENVIRONMENT_NAME,
      },
    });

    // Defines the initial function for cms news contents
    const fanAppCmsNewsInitialFunction = new lambda.Function(this, 'fanAppCmsUpdateNewsFunction', {
      runtime: lambda.Runtime.PYTHON_3_9, // execution environment
      code: lambda.Code.fromAsset('lib/functions/fan-app-cms'),
      handler: 'fan-app-cms-news.handler',
      tracing: lambda.Tracing.ACTIVE,
      timeout: cdk.Duration.seconds(600),
      memorySize: 1024,
      functionName: `${env.P13N}-cms-news-initial-data-load-${env.STAGE}`,
      layers: [props.lambdaCommonLayer],
      environment: {
        CONTENT_TABLE: content_table.tableName,
        CMS_API_KEY: env.CMS_API_KEY,
        CMS_ENDPOINT: CMS_CONFIG.cmsEndpoint,
        CMS_BASE_PATH: CMS_CONFIG.cmsBasePath,
        CDN_HOST: CMS_CONFIG.cdnHost,
        STAGE: env.STAGE,
        ENVIRONMENT_NAME: env.ENVIRONMENT_NAME,
      },
    });

    content_table.grantReadWriteData(fanAppThronInitialFunction);
    content_table.grantReadWriteData(fanAppCmsNewsInitialFunction);

    /* Granting Lmabda Access  keys and Secrets */
    fanAppThronInitialFunction.role?.attachInlinePolicy(
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
    thronConfigSecret.grantRead(fanAppThronInitialFunction);

    // steps for step function

    const fanAppInitialThronLoad = new tasks.LambdaInvoke(this, 'fanAppInitialThronLoad', {
      lambdaFunction: fanAppThronInitialFunction,
    });

    const fanAppInitialCmsNewsUpdate = new tasks.LambdaInvoke(this, 'fanAppInitialCmsNewsUpdate', {
      lambdaFunction: fanAppCmsNewsInitialFunction,
    });

    const initialDataLoad = new sfn.Parallel(this, 'initialDataLoad')
      .branch(fanAppInitialThronLoad)
      .branch(fanAppInitialCmsNewsUpdate);

    // STEP FUNCTION
    // Create the step function for the initial model training
    const fanAppInitialImportUserPreferences = new tasks.LambdaInvoke(
      this,
      'fanAppInitialImportUserPreferences',
      {
        lambdaFunction: initUserPreferencesDataImportFunction,
      },
    );

    const fanAppInitialGlueJob = new tasks.GlueStartJobRun(this, 'fanAppInitialGlueJob', {
      glueJobName: `${env.P13N}-user-behaviour-job-${env.STAGE}`,
    });

    const fanAppInitialImportContentData = new tasks.LambdaInvoke(
      this,
      'fanAppInitialImportContentData',
      {
        lambdaFunction: contentInitialIngestionLambda,
      },
    );

    const initialDataImport = new sfn.Parallel(this, 'initialDataImport')
      .branch(fanAppInitialImportUserPreferences)
      .branch(fanAppInitialGlueJob)
      .branch(fanAppInitialImportContentData);

    const waitForInitialImport = new sfn.Wait(this, 'fanAppInitialWaitForInitialImport', {
      time: sfn.WaitTime.duration(cdk.Duration.seconds(900)),
    });

    const createSolutionVersion = new tasks.LambdaInvoke(
      this,
      'fanAppInitialCreateSolutionVersion',
      {
        lambdaFunction: fanAppPersonalizeInitialSolution,
        // Lambda's returned result
        resultPath: '$',
      },
    );

    const waitForSolutionVersion = new sfn.Wait(this, 'fanAppInitialWaitForSolutionVersion', {
      time: sfn.WaitTime.duration(cdk.Duration.seconds(1800)),
    });

    const createUpdateCampaign = new tasks.LambdaInvoke(this, 'fanAppInitialCreateUpdateCampaign', {
      lambdaFunction: fanAppPersonalizeInitialCampaign,
      // Lambda's returned result
      resultPath: '$',
    });

    const createEventTracker = new tasks.LambdaInvoke(this, 'fanAppPersonalizeCreateEventTracker', {
      lambdaFunction: fanAppPersonalizeEventTracker,
      // Lambda's returned result
      resultPath: '$',
    });

    const endExecution = new sfn.Succeed(this, 'fanAppInitialReportSuccess');

    //const stateMachine =
    new sfn.StateMachine(this, 'fanAppInitialPersonalizeStateMachine', {
      definition: initialDataLoad
        .next(initialDataImport)
        .next(waitForInitialImport)
        .next(createSolutionVersion)
        .next(waitForSolutionVersion)
        .next(createUpdateCampaign)
        .next(createEventTracker)
        .next(endExecution),
      stateMachineName: `${env.P13N}-personalize-initial-state-machine-${env.STAGE}`,
    });

    // Update pipeline

    // Define a role for the Lambda updating the solution version
    const personalizeUpdateSolutionRole = new iam.Role(this, 'personalizeUpdateSolutionRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      roleName: `${env.P13N}-personalize-update-solution-role-${env.STAGE}`,
    });

    personalizeUpdateSolutionRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
    );

    personalizeUpdateSolutionRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaVPCAccessExecutionRole'),
    );

    // Give the Lambda access to Amazon Personalize
    personalizeUpdateSolutionRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AmazonPersonalizeFullAccess'),
    );

    // Give the Lambda access to SSM
    personalizeUpdateSolutionRole.attachInlinePolicy(ssmPolicy);

    // Defines the initial personalize function to update the solution version
    const fanAppPersonalizeUpdateSolution = new lambda.Function(
      this,
      'fanAppPersonalizeUpdateSolution',
      {
        runtime: lambda.Runtime.PYTHON_3_9, // execution environment
        code: lambda.Code.fromAsset('lib/functions/fan-app-personalize'),
        handler: 'fan-app-personalize-update-solution.handler',
        tracing: lambda.Tracing.ACTIVE,
        timeout: cdk.Duration.seconds(600),
        functionName: `${env.P13N}-personalize-update-solution-${env.STAGE}`,
        role: personalizeUpdateSolutionRole,
        environment: {
          STAGE: env.STAGE,
          ENVIRONMENT_NAME: env.ENVIRONMENT_NAME,
        },
      },
    );

    // Define a role for the Lambda creating the solution
    const personalizeUpdateCampaignRole = new iam.Role(this, 'personalizeUpdateCampaignRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      roleName: `${env.P13N}-personalize-update-campaign-role-${env.STAGE}`,
    });

    personalizeUpdateCampaignRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
    );

    personalizeUpdateCampaignRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaVPCAccessExecutionRole'),
    );

    // Give the Lambda access to Amazon Personalize
    personalizeUpdateCampaignRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AmazonPersonalizeFullAccess'),
    );
    // Give the Lambda access to SSM
    personalizeUpdateCampaignRole.attachInlinePolicy(ssmPolicy);

    // Defines the initial personalize function to create or update the campaign
    const fanAppPersonalizeUpdateCampaign = new lambda.Function(
      this,
      'fanAppPersonalizeUpdateCampaign',
      {
        runtime: lambda.Runtime.PYTHON_3_9, // execution environment
        code: lambda.Code.fromAsset('lib/functions/fan-app-personalize'),
        handler: 'fan-app-personalize-update-campaign.handler',
        tracing: lambda.Tracing.ACTIVE,
        timeout: cdk.Duration.seconds(600),
        functionName: `${env.P13N}-personalize-update-campaign-${env.STAGE}`,
        role: personalizeUpdateCampaignRole,
        environment: {
          STAGE: env.STAGE,
          ENVIRONMENT_NAME: env.ENVIRONMENT_NAME,
        },
      },
    );

    // Create the step function for the new model training
    const createNewSolutionVersion = new tasks.LambdaInvoke(this, 'fanAppUpdateSolutionVersion', {
      lambdaFunction: fanAppPersonalizeUpdateSolution,
      // Lambda's returned result
      resultPath: '$',
    });

    const waitForNewSolutionVersion = new sfn.Wait(this, 'fanAppUpdateWaitForSolutionVersion', {
      time: sfn.WaitTime.duration(cdk.Duration.seconds(1800)),
    });

    const updateCampaign = new tasks.LambdaInvoke(this, 'fanAppUpdateCampaign', {
      lambdaFunction: fanAppPersonalizeUpdateCampaign,
      // Lambda's returned result
      resultPath: '$',
    });

    const endUpdateExecution = new sfn.Succeed(this, 'fanAppUpdateReportSuccess');

    //const stateMachine =
    const UpdateStateMachine = new sfn.StateMachine(this, 'fanAppUpdatePersonalizeStateMachine', {
      definition: createNewSolutionVersion
        .next(waitForNewSolutionVersion)
        .next(updateCampaign)
        .next(endUpdateExecution),
      stateMachineName: `${env.P13N}-personalize-update-state-machine-${env.STAGE}`,
    });

    // Create EventBridge rule to trigger update pipeline on daily (/weekly) basis

    // define target first
    const StepFunctionTarget = new events_targets.SfnStateMachine(UpdateStateMachine);
    // create rule
    new Rule(this, 'TriggerUpdatePipelineRule', {
      schedule: Schedule.cron({ minute: '0', hour: '4', weekDay: '*' }), // every day at 4 am UTC
      targets: [StepFunctionTarget],
    });
  }
}
