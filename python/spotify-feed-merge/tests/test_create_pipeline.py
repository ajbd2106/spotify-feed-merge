#!/usr/bin/env python

import apache_beam
import unittest

from spotifyfeedmerge import CreatePipeline
from spotifyfeedmerge import SetPipelineOptions

class TestCreatePipeline(unittest.TestCase):
    def setUp(self): 
        return None

    def test_pipeline_create(self):
        pipeline = CreatePipeline(options=None)
        # bacon = Bacon(pipeline.pipeline)
        # print(bacon)
        self.assertIsInstance(pipeline, apache_beam.Pipeline) 

    def test_pipeline_options(self):
        spo = SetPipelineOptions()
        google_cloud = spo.config.get('google_cloud')
        spo.options = spo.set_pipeline()
        spo.set_google_cloud_options(google_cloud, spo.options)
        options = spo.set_runner(spo.options, spo.config.get('standard').get('runner')) 
        pipeline = CreatePipeline(options=options)
        self.assertIsInstance(pipeline, apache_beam.Pipeline)
