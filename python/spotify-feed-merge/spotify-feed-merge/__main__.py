#!/usr/bin/env python

import apache_beam
import configobj
import json

from .users import ProcessUsers
from .users import ReadUsers

class CreatePipeline(apache_beam.Pipeline):
    pipeline = []

    def __init__(self, options):
        self.pipeline = apache_beam.Pipeline(options=options)


class GroupStreams:
    try:
        denormalized_path = configobj.ConfigObj("sfm/sfm.conf").get('standard').get('denormalized')
    except:
        denormalized_path = configobj.ConfigObj("sfm.conf").get('standard').get('denormalized')

    def group_streams_with_tracks(self, pipeline, streams, tracks):
        return (({'streams': streams, 'tracks': tracks})
            | 'co group by key track_id' >> apache_beam.CoGroupByKey(pipeline=pipeline)
        )

    def group_streams_with_users(self, streams, users):
        return (({'streams': streams, 'users': users})
            | 'co group by key user_id' >> apache_beam.CoGroupByKey()
        ) 

    def output_result(self, streams):
        return (streams
             | 'output' >> apache_beam.io.WriteToText(self.denormalized_path)
        )

    def remap_streams(self, streams):
        return (streams
            | 'remap streams' >> apache_beam.Map(lambda streams: (json.loads(streams).get('track_id'), json.loads(streams)))
        )


class ProcessTracks(apache_beam.DoFn):
    def process(self, element):
        key, values = element
        tracks = values.get('tracks')[0]
        for k, v in enumerate(values.get('streams')):
            values.get('streams')[k].update({
                'album_code': tracks.get('album_code'),
                'isrc': tracks.get('isrc')
            })
        return values.get('streams') 

class ProcessUsers(apache_beam.DoFn):
    def process(self, element):
        key, values = element
        users = values.get('users')[0]
        for k, v in enumerate(values.get('streams')):
            values.get('streams')[k].update({
                'product':users.get('product'),
                'country':users.get('country'),
                'region':users.get('region'),
                'zip_code':users.get('zip_code'),
                'access':users.get('access'),
                'gender':users.get('gender'),
                'partner':users.get('partner'),
                'referral':users.get('referral'),
                'type':users.get('type'),
                'birth_year':users.get('birth_year')
            })
        # values.get('streams').pop(0)
        return values.get('streams')


class ReadUsers:
    try:
        users_path = configobj.ConfigObj("sfm/sfm.conf").get('standard').get('users') 
    except:
        users_path = configobj.ConfigObj("sfm.conf").get('standard').get('users') 

    def read_users(self, pipeline):
        return (pipeline
            | 'read users' >> apache_beam.io.ReadFromText(self.users_path)
        )
    
    def map_users(self, pipeline):
        return (pipeline
            | 'map users' >> apache_beam.Map(lambda user_id: (json.loads(user_id).get('user_id'), json.loads(user_id)))
        )
class ReadTracks:
    try:
        tracks_path = configobj.ConfigObj("sfm/sfm.conf").get('standard').get('tracks')
    except:
        tracks_path = configobj.ConfigObj("sfm.conf").get('standard').get('tracks')

    def read_tracks(self, pipeline):
        return (pipeline
            | 'read tracks' >> apache_beam.io.ReadFromText(self.tracks_path)
        )     

    def map_tracks(self, pipeline):
        return (pipeline
            | 'map tracks' >> apache_beam.Map(lambda track: (json.loads(track).get('track_id'), json.loads(track)))
        )
    
class ReadStreams:
    try:
        streams_path = configobj.ConfigObj("sfm/sfm.conf").get('standard').get('streams')    
    except:
        streams_path = configobj.ConfigObj("sfm.conf").get('standard').get('streams')    

    def read_streams(self, pipeline):
        return (pipeline 
             | 'read streams' >> apache_beam.io.ReadFromText(self.streams_path)
        ) 

    def map_streams(self, pipeline):
        return (pipeline
             | 'map streams' >> apache_beam.Map(lambda user_id: (json.loads(user_id).get('user_id'), json.loads(user_id)))
        )


class SetPipelineOptions:
    config = configobj.ConfigObj("sfm.conf")
    options = apache_beam.utils.pipeline_options.PipelineOptions

    def __init__(self):
        self.google_cloud = SetPipelineOptions.config.get("google_cloud")
        self.runner = SetPipelineOptions.config.get('standard').get('runner')

    def set_google_cloud_options(self, google_cloud, pipeline):
        options = pipeline.view_as(apache_beam.utils.pipeline_options.GoogleCloudOptions) 
        options.project = google_cloud.get("project") 
        options.staging_location = google_cloud.get("staging") 
        options.job_name = "sfm"
        options.temp_location = google_cloud.get("temp") 
        return options 

    def set_pipeline(self):
        return apache_beam.utils.pipeline_options.PipelineOptions()

    def set_runner(self, options, runner):
        options = options.view_as(apache_beam.utils.pipeline_options.StandardOptions)
        options.runner = runner 
        return options

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
