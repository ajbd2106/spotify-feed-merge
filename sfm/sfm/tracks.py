#!/usr/bin/env python

import apache_beam
import json
import configobj

from options import SetPipelineOptions

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
