#!/usr/bin/env python

import apache_beam
import configobj

from options import SetPipelineOptions

class ReadTracks:
    tracks_path = configobj.ConfigObj("sfm.conf").get('standard').get('tracks_path')

    def read_tracks(self, pipeline):
        return (pipeline
            | 'read tracks' >> apache_beam.io.ReadFromText(self.tracks_path)
        )     

    def map_tracks(self, pipeline):
        pass
    

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
