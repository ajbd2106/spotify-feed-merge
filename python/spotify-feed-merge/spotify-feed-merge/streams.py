#!/usr/bin/env python

import apache_beam
import configobj
import json

from options import SetPipelineOptions

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