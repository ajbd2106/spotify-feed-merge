#!/usr/bin/env python

import apache_beam
import sys
import unittest

# For another time.
# from apache_beam.pipeline_test import Bacon

from create_pipeline import CreatePipeline
from options import SetPipelineOptions
from streams import GroupStreams
from streams import ReadStreams
from tracks import ProcessTracks
from tracks import ReadTracks
from users import ProcessUsers
from users import ReadUsers
        
    
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
        

class TestReads(unittest.TestCase):
    pipeline = ""

    def setUp(self):
        options = SetPipelineOptions()
        google_cloud = options.config.get('google_cloud')
        runner = options.config.get('standard').get('runner')
        options.pipeline = options.set_pipeline()
        options.set_google_cloud_options(google_cloud, options.pipeline)
        options = options.set_runner(options.pipeline, runner)
        self.pipeline = CreatePipeline(options=options).pipeline

    def test_read_streams(self):
        rs = ReadStreams()    
        streams = rs.read_streams(self.pipeline)
        self.assertIsInstance(streams, apache_beam.pvalue.PCollection)
    
    def test_map_streams(self):
        rs = ReadStreams()
        streams = rs.read_streams(self.pipeline)
        streams = rs.map_streams(streams)
        self.assertIsInstance(streams, apache_beam.pvalue.PCollection)

    def test_read_users(self):
        ru = ReadUsers()
        users = ru.read_users(self.pipeline)
        self.assertIsInstance(users, apache_beam.pvalue.PCollection)

    def test_map_users(self):
        ru = ReadUsers()
        users = ru.read_users(self.pipeline)
        users = ru.map_users(users)
        self.assertIsInstance(users, apache_beam.pvalue.PCollection)

    def test_read_tracks(self):
        rt = ReadTracks()
        tracks = rt.read_tracks(self.pipeline)
        self.assertIsInstance(tracks, apache_beam.pvalue.PCollection)        

    def test_map_tracks(self):
        rt = ReadTracks()
        tracks = rt.read_tracks(self.pipeline)
        tracks = rt.map_tracks(self.pipeline)
        self.assertIsInstance(tracks, apache_beam.pvalue.PCollection) 

class TestProcessing(unittest.TestCase):
    pipeline = ""
    streams = ""
    tracks = ""
    users = ""

    def setUp(self):
        options = SetPipelineOptions()
        google_cloud = options.config.get('google_cloud')
        runner = options.config.get('standard').get('runner')
        options.pipeline = options.set_pipeline()
        options.set_google_cloud_options(google_cloud, options.pipeline)
        options = options.set_runner(options.pipeline, runner)
        self.pipeline = CreatePipeline(options=options).pipeline
    
        # Prepare the streams pipeline.
        rs = ReadStreams()
        self.streams = rs.read_streams(self.pipeline)
        self.streams = rs.map_streams(self.streams)

        # Prepare the tracks pipeline.
        rt = ReadTracks()
        self.tracks = rt.read_tracks(self.pipeline)
        self.tracks = rt.map_tracks(self.tracks)

        # Prepare the users pipeline.
        ru = ReadUsers()
        self.users = ru.read_users(self.pipeline)
        self.users = ru.map_users(self.users)

    def test_group_streams_with_users(self):
        group_by = GroupStreams()
        grouped_result = group_by.group_streams_with_users(self.streams, self.users)
        self.assertIsInstance(grouped_result, apache_beam.pvalue.PCollection) 

    def test_process_users(self):
        group_by = GroupStreams()
        streams_with_users = group_by.group_streams_with_users(self.streams, self.users)
        process_users = streams_with_users | 'process users' >> apache_beam.ParDo(ProcessUsers())
        self.assertIsInstance(process_users, apache_beam.pvalue.PCollection)

    def test_remap_streams(self):
        gs = GroupStreams()
        streams_with_users = gs.group_streams_with_users(self.streams, self.users)
        process_users = streams_with_users | 'process users' >> apache_beam.ParDo(ProcessUsers())
        self.streams = gs.remap_streams(self.streams)
        self.assertIsInstance(self.streams, apache_beam.pvalue.PCollection)
    
    def test_group_streams_with_tracks(self):
        gs = GroupStreams()
        grouped_result = gs.group_streams_with_tracks(self.pipeline, self.streams, self.tracks)
        self.assertIsInstance(grouped_result, apache_beam.pvalue.PCollection) 

    def test_process_tracks(self):
        gs = GroupStreams()
        streams_with_tracks = gs.group_streams_with_tracks(self.pipeline, self.streams, self.users)
        process_tracks = streams_with_tracks | 'process tracks' >> apache_beam.ParDo(ProcessTracks())
        self.assertIsInstance(process_tracks, apache_beam.pvalue.PCollection)

    def test_output_result(self):
        gs = GroupStreams()
        streams_with_tracks = gs.group_streams_with_tracks(self.pipeline, self.streams, self.users)
        process_tracks = streams_with_tracks | 'process tracks' >> apache_beam.ParDo(ProcessTracks())
        result = gs.output_result(process_tracks)
        self.assertIsInstance(result, apache_beam.pvalue.PCollection)


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
