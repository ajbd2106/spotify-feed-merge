#!/usr/bin/env python

import apache_beam

class ReadStreams:
    def __init__(self):
        pass

    def read_streams(self, pipeline):
        return pipeline 
             | 'read streams' >> apache_beam.io.ReadFromText('gs://abbynormal/streams.gz') 

    def map_steams(self, pipeline):
        return pipeline
             | 'map streams' >> apache_beam.Map(lambda user_id: (json.loads(user_id).get('user_id'), json.loads(user_id)))
