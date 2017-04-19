#!/usr/bin/env python

import apache_beam
import configobj
import os

from options import SetPipelineOptions

class ReadStreams:
    streams_path = configobj.ConfigObj("/python/sfm.conf").get('standard').get('streams')    

    def read_streams(self, pipeline):
        return (pipeline 
             | 'read streams' >> apache_beam.io.ReadFromText(self.streams_path)
        ) 

    def map_streams(self, pipeline):
        return (pipeline
             | 'map streams' >> apache_beam.Map(lambda user_id: (json.loads(user_id).get('user_id'), json.loads(user_id)))
        )
