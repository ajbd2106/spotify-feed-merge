#!/usr/bin/env python

import apache_beam
import sys
import unittest

import sfm

from test_create_pipeline import TestCreatePipeline
from test_options import TestOptions
from test_reads import TestReads
from test_processing import TestProcessing

test_create_pipeline = unittest.TestLoader().loadTestsFromTestCase(TestCreatePipeline)
test_options = unittest.TestLoader().loadTestsFromTestCase(TestOptions)
test_reads = unittest.TestLoader().loadTestsFromTestCase(TestReads)
test_processing = unittest.TestLoader().loadTestsFromTestCase(TestProcessing)

suite = unittest.TestSuite([
    test_create_pipeline,
    test_options,
    test_reads,
    test_processing,
])
results = unittest.TextTestRunner(verbosity=5).run(suite)

if len(results.failures) > 0 or len(results.errors) > 0:
    sys.exit(143)
