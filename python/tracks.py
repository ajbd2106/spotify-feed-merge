#!/usr/bin/env python

class ProcessTracks(ab.DoFn):
    def process(self, element):
        key, values = element
        tracks = values.get('tracks')[0]
        for k, v in enumerate(values.get('streams')):
            values.get('streams')[k].update({
                'album_code': tracks.get('album_code'),
                'isrc': tracks.get('isrc')
            })
        return values.get('streams') 
