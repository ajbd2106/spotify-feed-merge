#!/usr/bin/env python

import apache_beam
import unittest

from spotifyfeedmerge import CreatePipeline
from spotifyfeedmerge import ReadStreams
from spotifyfeedmerge import ReadTracks
from spotifyfeedmerge import ReadUsers
from spotifyfeedmerge import SetPipelineOptions

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
