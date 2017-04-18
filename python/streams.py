#!/usr/bin/env python

import apache_beam

from options import SetPipelineOptions

class ReadStreams:
    def __init__(self):
        pass

    def read_streams(self, pipeline, options):
        return (pipeline 
             | 'read streams' >> apache_beam.io.ReadFromText(options.streams)
        ) 

    def map_steams(self, pipeline):
        return (pipeline
             | 'map streams' >> apache_beam.Map(lambda user_id: (json.loads(user_id).get('user_id'), json.loads(user_id)))
        )
