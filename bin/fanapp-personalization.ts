#!/usr/bin/env node
import * as cdk from '@aws-cdk/core';
import { DefaultStackSynthesizer } from '@aws-cdk/core';
import {
  FanAppPersonalizationStack,
  DataIngestionContentStack,
  DataProcessingContentStack,
} from '../lib/stacks';

import { env } from '../lib/utils/env-variables';

const ACCOUNT = process.env.CDK_DEPLOY_ACCOUNT ?? process.env.CDK_DEFAULT_ACCOUNT;
if (!ACCOUNT) throw new Error('Unable to determine deployment account');
const REGION = process.env.CDK_DEPLOY_REGION ?? process.env.CDK_DEFAULT_REGION;
if (!REGION) throw new Error('Unable to determine deployment region');

const trunkBranches = ['dev', 'test', 'staging', 'prod']; // trunk branches are those for which ci-cd pipelines are defined.

// To reference the personalise data bucket in other stacks props
// Use FanAppPersonalizationStack.fanAppPersonalisationBucket
const fanAppPersonalizationStackName = trunkBranches.includes(env.STAGE)
  ? `${env.P13N}-stack`
  : `${env.P13N}-stack-${env.STAGE}`; // if it is not a ci-cd pipeline

const dataIngestionContentStackName = trunkBranches.includes(env.STAGE)
  ? `${env.P13N}-data-ingestion-content-stack`
  : `${env.P13N}-data-ingestion-content-stack-${env.STAGE}`; // if it is not a ci-cd pipeline

const dataProcessingContentStackName = trunkBranches.includes(env.STAGE)
  ? `${env.P13N}-data-processing-content-stack`
  : `${env.P13N}-data-processing-content-stack-${env.STAGE}`; // if it is not a ci-cd pipeline

// const terminationProtection = env.ENVIRONMENT_NAME != 'dev';

const app = new cdk.App();

// Main personalization stack;
const fanAppPersonalizationStack = new FanAppPersonalizationStack(
  app,
  fanAppPersonalizationStackName,
  {
    env: {
      account: ACCOUNT,
      region: REGION,
    },
    synthesizer: new DefaultStackSynthesizer({
      qualifier: 'fanapp', //re-using existing fanapp qualifier
    }),
  },
);

// Nested stack; No need to speicfy a synthetizer for nested stacks;
new DataIngestionContentStack(fanAppPersonalizationStack, dataIngestionContentStackName, {
  fanAppContentDdbTableName: fanAppPersonalizationStack.fanAppContentDdbTableName,
  lambdaCommonLayer: fanAppPersonalizationStack.commonLambdaLayer,
});

// Nested stack; No need to speicfy a synthetizer for nested stacks;
new DataProcessingContentStack(fanAppPersonalizationStack, dataProcessingContentStackName, {
  fanAppPersonalisationBucket: fanAppPersonalizationStack.fanAppPersonalisationBucket,
  fanAppPersonalisationImportRole: fanAppPersonalizationStack.fanAppPersonalisationImportRole,
  fanAppPersonalisationVideoDatasetGroup:
    fanAppPersonalizationStack.fanAppPersonalisationVideoDatasetGroup,
  fanAppPersonalisationNewsDatasetGroup:
    fanAppPersonalizationStack.fanAppPersonalisationNewsDatasetGroup,
  fanAppContentDdbTableName: fanAppPersonalizationStack.fanAppContentDdbTableName,
  fanAppContentDdbTable: fanAppPersonalizationStack.fanAppContentDdbTable,
  lambdaCommonLayer: fanAppPersonalizationStack.commonLambdaLayer,
});

cdk.Tags.of(app).add('environment', env.ENVIRONMENT_NAME);
cdk.Tags.of(app).add('stage', env.STAGE);
cdk.Tags.of(app).add('app', 'FanApp');
