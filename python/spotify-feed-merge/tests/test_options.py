#!/usr/bin/env python

import apache_beam
import unittest

import spotifyfeedmerge 

class TestOptions(unittest.TestCase):
    def setUp(self):
        return None 

    def test_instantiate_class(self):
        options = SetPipelineOptions() 
        self.assertIsInstance(options, SetPipelineOptions) 

    def test_set_google_cloud_options(self):
        gco = SetPipelineOptions()
        google_cloud = gco.config.get("google_cloud")
        gco.pipeline = gco.set_pipeline()
        results = gco.set_google_cloud_options(google_cloud, gco.pipeline)
        self.assertIsInstance(results, apache_beam.utils.pipeline_options.GoogleCloudOptions)

    def test_set_pipeline(self): 
        pipeline = SetPipelineOptions().set_pipeline() 
        self.assertIsInstance(pipeline, apache_beam.utils.pipeline_options.PipelineOptions)

    def test_set_runner(self):
        set_pipeline_options = SetPipelineOptions()
        options = set_pipeline_options.set_pipeline() 
        runner = set_pipeline_options.set_runner(options, 'DataflowRunner')
        self.assertIsInstance(runner, apache_beam.utils.pipeline_options.PipelineOptions)
        

