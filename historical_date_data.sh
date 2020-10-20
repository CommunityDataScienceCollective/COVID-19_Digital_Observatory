#!/bin/bash

for line in $(cat missing_view_data); do 
	OVERRIDE_DATE_STRING=${line} bash ./cron-wikipedia_views.sh
done

