# -*- coding: utf-8 -*-
"""
Created on Tue Aug 22 23:27:44 2017

@author: Praca
"""

def createPackage(host, user, auth, s3_path, project_name, modules):
    import paramiko
    
    install_commands = ""
    for module in modules:
        install_commands += 'python3.6 -m pip install ' + module + '; '
    
    k = paramiko.RSAKey.from_private_key_file(auth)
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    print("Connecting to " + host)
    c.connect( hostname = host, username = user, pkey = k )
    print("Connected to " + host)
    
    commands = [
        'sudo rm -r home/ubuntu/virtualenv/'+project_name+'Env',
        'virtualenv -p /usr/bin/python3.6 /home/ubuntu/virtualenv/'+project_name+'Env',
        'source /home/ubuntu/virtualenv/'+project_name+'Env'+'/bin/activate; ' + 
        install_commands +
        'deactivate',
        'cp -r /home/ubuntu/virtualenv/'+project_name+'Env'+'/lib/python3.6/site-packages/. /home/ubuntu/scripts/lambda/'+project_name+'/',
        'cd /home/ubuntu/scripts/lambda/'+project_name+'/; '+
        'zip -r /home/ubuntu/scripts/lambda/'+project_name+'.zip ./.; ' + 
        'cd -',
        'aws s3 cp /home/ubuntu/scripts/lambda/'+project_name+'.zip s3://' + s3_path
        ]
    for command in commands:
        print("Executing {}".format(command))
        stdin , stdout, stderr = c.exec_command(command)
        print(stdout.read())
        print(stderr.read())
    c.close()


host = 'ec2-52-212-160-92.eu-west-1.compute.amazonaws.com'
user = 'ubuntu'
auth = 'ETLFacebook.pem'
s3_path = 'deepdoccodes/produkcja/ETL\ Facebook/'



#project_name = 'FBLambdaProcessImages'
#modules = []
#createPackage(host, user, auth, s3_path, project_name, modules)
#
#project_name = 'FBLambdaGetHSVData'
#modules = ['pillow', 'numpy']
#createPackage(host, user, auth, s3_path, project_name, modules)

#project_name = 'FBLambdaGetRekognitionData'
#modules = []
#createPackage(host, user, auth, s3_path, project_name, modules)

project_name = 'FBLambdaInvokeEC2ETL'
modules = ['cryptography', 'paramiko']
createPackage(host, user, auth, s3_path, project_name, modules)
#
#project_name = 'FBLambdaUploadImageProcessingDataToDB'
#modules = ['time', 'sqlalchemy', 'pymysql']
#createPackage(host, user, auth, s3_path, project_name, modules)