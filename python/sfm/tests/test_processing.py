#!/usr/bin/env python

import unittest

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
