#!/usr/bin/env python

from __future__ import print_function

import apache_beam as ab
import json

from apache_beam.pvalue import AsList

class ProcessUsers(ab.DoFn):
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

# I amuse myself.
#def process_users(u, s):
#    for i,v in enumerate(s):
#        if s[i].get('user_id') == u.get('user_id'):
#            s[i].update({
#                'product':u.get('product'),
#                'country':u.get('country'),
#                'region':u.get('region'),
#                'zip_code':u.get('zip_code'),
#                'access':u.get('access'),
#                'gender':u.get('gender'),
#                'partner':u.get('partner'),
#                'referral':u.get('referral'),
#                'type':u.get('type'),
#                'birth_year':u.get('birth_year')
#            })
#    return s 

o = ab.utils.pipeline_options.PipelineOptions()
gco = o.view_as(ab.utils.pipeline_options.GoogleCloudOptions)
gco.project = 'umg-technical-evaluation'
gco.staging_location = 'gs://abbynormal-staging/stage'
gco.temp_location = 'gs://abbynormal-staging/tmp'
o.view_as(ab.utils.pipeline_options.StandardOptions).runner = 'DataflowRunner'

p = []
p = ab.Pipeline(options=o)
s = (p 
     | 'read streams' >> ab.io.ReadFromText('gs://abbynormal/streams.gz') 
     | 'map streams' >> ab.Map(lambda user_id: (json.loads(user_id).get('user_id'), json.loads(user_id)))
)

u = (p
     | 'read users' >> ab.io.ReadFromText('gs://abbynormal/users.gz')
     | 'map users' >> ab.Map(lambda user_id: (json.loads(user_id).get('user_id'), json.loads(user_id)))
)

t = (p
     | 'read tracks' >> ab.io.ReadFromText('gs://abbynormal/tracks.gz')
     | 'map tracks' >> ab.Map(lambda track_id: (json.loads(track_id).get('track_id'), json.loads(track_id)))
)

s = (({'streams':s,'users':u}) | 'co group by key users' >> ab.CoGroupByKey())
s = s | 'process users' >> ab.ParDo(ProcessUsers())
s = s | 'remap streams' >> ab.Map(lambda sid: (sid.get('track_id'), sid))
s = (({'streams':s, 'tracks':t}) | 'co group by key tracks' >> ab.CoGroupByKey(pipeline=p))
s = s | 'process tracks' >> ab.ParDo(ProcessTracks())
#s = s | 'print st' >> ab.Map(lambda sid: print(sid))
s = s | 'output' >> ab.io.WriteToText('gs://abbynormal/dnormal')

p.run()
