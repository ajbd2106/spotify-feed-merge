#!/usr/bin/env python

import apache_beam
import configobj
import json

from options import SetPipelineOptions

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
