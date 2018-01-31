# -*- coding: utf-8 -*-
"""
Created on Mon Jun 19 11:21:20 2017

@author: Mikołaj
"""

############################################################
################## RDS CONFIG ##############################
rds_host = 'deepdoc.ctjllxyunqbm.eu-west-1.rds.amazonaws.com'
db_username = "dd_pawel"
db_password = "Lato17"
db_name = "deepdoc3" 

photos_table = 'photos'

labels_table = 'photosAWSLabels'
faces_table = 'photosAWSFaces'
hsv_table = 'photosHSV'



############################################################
################### S3 CONFIG ##############################

source_data_bucket='deepdocfbdata'

tempcsvpath='/tmp/file.csv'
tempjpgpath='/tmp/file.jpg'

#tempcsvpath='D:/DeepDoc/Sandbox/file.csv'
#tempjpgpath='D:/DeepDoc/Sandbox/file.jpg'
