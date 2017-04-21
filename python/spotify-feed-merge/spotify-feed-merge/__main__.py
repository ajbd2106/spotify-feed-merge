#!/usr/bin/env python

import apache_beam
import json

from .create_pipeline import CreatePipeline
from .options import SetPipelineOptions

from .streams import GroupStreams
from .streams import ReadStreams

from .tracks import ProcessTracks
from .tracks import ReadTracks

from .users import ProcessUsers
from .users import ReadUsers

def main():
    spo = SetPipelineOptions()
    spo.options = spo.set_pipeline()
    options = spo.set_google_cloud_options(spo.google_cloud, spo.options)
    options = spo.set_runner(spo.options, spo.config.get('standard').get('runner'))
    pipeline = CreatePipeline(options).pipeline

    read_streams = ReadStreams()

    streams_pc = read_streams.read_streams(pipeline)
    streams_pc = read_streams.map_streams(streams_pc)

    print(streams_pc)

    read_users = ReadUsers()

    users_pc = read_users.read_users(pipeline)
    users_pc = read_users.map_users(users_pc)

    print(users_pc)

    read_tracks = ReadTracks()

    tracks_pc = read_tracks.read_tracks(pipeline)
    tracks_pc = read_tracks.map_tracks(tracks_pc)

    print(tracks_pc)

    group_streams = GroupStreams()

    streams_pc = group_streams.group_streams_with_users(streams_pc, users_pc)
    streams_pc = streams_pc | 'process users' >> apache_beam.ParDo(ProcessUsers())
    streams_pc = group_streams.remap_streams(streams_pc)
    streams_pc = streams_pc | 'process tracks' >> apache_beam.ParDo(ProcessTracks())
    streams_pc = group_streams.group_streams_with_tracks(pipeline, streams_pc, tracks_pc)
    streams_pc = group_streams.output_result(streams_pc)

    pipeline.run()

if __name__ == '__main__':
    main()
