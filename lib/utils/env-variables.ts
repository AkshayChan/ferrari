/*
 * © 2021 Amazon Web Services, Inc. or its affiliates. All Rights Reserved.
 * This AWS Content is provided subject to the terms of the AWS Customer Agreement
 * available at http://aws.amazon.com/agreement or other written agreement between Customer
 * and either Amazon Web Services, Inc. or Amazon Web Services EMEA SARL or both
 */

const envVars = {
  ENVIRONMENT_NAME: 'dev', // to which environment it should be integrated
  STAGE: 'dev', // name of the stage distinguish resources
  P13N: 'fan-app-p13n',
  CMS_ENV: 'test',
  THRON_ENV: 'test',
  CMS_API_KEY: null,
  PINPOINT_KEY: null,
  DDB_PROF_STR: null,
} as const;

export const env = Object.entries(envVars).reduce(
  (acc, [name, defaultValue]) => {
    // eslint-disable-next-line security/detect-object-injection
    const value = process.env[name] ?? defaultValue;
    if (value == null) throw Error(`Environment variable ${name} must be set`);
    return Object.assign(acc, { [name]: value });
  },
  {} as {
    [key in keyof typeof envVars]: string;
  },
);
