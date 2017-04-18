#!/usr/bin/env python

import apache_beam
import configobj

from options import SetPipelineOptions

class ReadStreams:
    streams_path = configobj.ConfigObj("sfm.conf").get('standard').get('streams')    

    def read_streams(self, pipeline):
        streams = pipeline | 'read streams path' >> apache_beam.io.ReadFromText(self.streams_path)
        return (pipeline 
             | 'read streams' >> apache_beam.io.ReadFromText(self.streams)
        ) 

    def map_steams(self, pipeline):
        return (pipeline
             | 'map streams' >> apache_beam.Map(lambda user_id: (json.loads(user_id).get('user_id'), json.loads(user_id)))
        )
