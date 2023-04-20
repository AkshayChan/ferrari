# © 2022 Amazon Web Services, Inc. or its affiliates. All Rights Reserved. This
# AWS Content is provided subject to the terms of the AWS Customer Agreement
# available at http://aws.amazon.com/agreement or other written agreement between
# Customer and either Amazon Web Services, Inc. or Amazon Web Services EMEA SARL
# or both.

# Any code, applications, scripts, templates, proofs of concept, documentation
# and other items provided by AWS under this SOW are "AWS Content," as defined
# in the Agreement, and are provided for illustration purposes only. All such
# AWS Content is provided solely at the option of AWS, and is subject to the
# terms of the Addendum and the Agreement. Customer is solely responsible for
# using, deploying, testing, and supporting any code and applications provided
# by AWS under this SOW.

import http.client
import logging


def initHttpClientDebugLevel(level=3):
   log = logging.getLogger('urllib3')
   log.setLevel(logging.INFO)

   # logging from urllib3 to console
   ch = logging.StreamHandler()
   ch.setLevel(logging.INFO)
   log.addHandler(ch)
   # print statements from `http.client.HTTPConnection` to console/stdout
   http.client.HTTPSConnection.debuglevel = level