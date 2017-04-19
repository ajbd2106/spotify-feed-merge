#!/usr/bin/env python

import apache_beam

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
