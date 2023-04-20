/* eslint-disable security/detect-object-injection */
/*
 * © 2022 Amazon Web Services, Inc. or its affiliates. All Rights Reserved.
 * This AWS Content is provided subject to the terms of the AWS Customer Agreement
 * available at http://aws.amazon.com/agreement or other written agreement between Customer
 * and either Amazon Web Services, Inc. or Amazon Web Services EMEA SARL or both
 */

export type thronEnvConfig = {
  thronHost: string;
  thronAdminHost: string;
  thronPublicFolder: string;
  thronConfigSecretArn: string;
};

export type cmsEnvConfig = {
  cmsEndpoint: string;
  cmsBasePath: string;
  cdnHost: string;
  cmsApiKeySecretArn: string;
};

export const cmsEnvs: Record<string, cmsEnvConfig> = {};
export const thronEnvs: Record<string, thronEnvConfig> = {};

cmsEnvs['test'] = {
  cmsEndpoint: 'https://api.test.ferrari.com',
  cmsBasePath: 'https://api.test.ferrari.com/cms/network',
  cdnHost: 'https://cdn.test.ferrari.com/cms/network/media/img/resize',
  cmsApiKeySecretArn:
    'arn:aws:secretsmanager:eu-west-1:213728519673:secret:ferrari-cms-test-api-key-xlgjw5',
};

thronEnvs['prod'] = {
  thronHost: 'https://ferrari-view.thron.com/api/xcontents/resources/delivery',
  thronAdminHost: 'https://ferrari-view.thron.com/api',
  thronPublicFolder: '49817556-42b2-46ac-af13-aa2b84bdb1bd',
  thronConfigSecretArn:
    'arn:aws:secretsmanager:eu-west-1:213728519673:secret:fanapp-thron-p13n-ingestion-7biUZG',
};

thronEnvs['test'] = thronEnvs['prod'];

cmsEnvs['prod'] = {
  cmsEndpoint: 'https://api.ferrari.com',
  cmsBasePath: 'https://api.ferrari.com/cms/network',
  cdnHost: 'https://cdn.ferrari.com/cms/network/media/img/resize',
  cmsApiKeySecretArn:
    'arn:aws:secretsmanager:eu-west-1:213728519673:secret:ferrari-cms-prod-api-key-n4ufQl',
};
