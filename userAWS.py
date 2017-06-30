# -*- coding: utf-8 -*-
"""
Created on Thu Jun 29 20:34:42 2017

@author: Mikołaj
"""
'''zmiana z mastera'''


###### policy
    
DevAccess={
   "Version":"2012-10-17",
   "Statement":[
   {
      "Effect": "Allow",
      "Action": "iam:GetAccountPasswordPolicy",
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": "iam:ChangePassword",
      "Resource": "arn:aws:iam::050452088192:user/${aws:username}"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:Get*",
        "s3:List*"
      ],
      "Resource": "*"
    },        
   ]
}   
iam.create_policy(
    PolicyName='DevAccess',
    PolicyDocument=json.dumps(DevAccess),
    Description='Dostępy do wybranych bucketow s3, lambdy i ec2'
)


####### user ####################

username='Pawel'

def createUser(username):
    iam.create_user(
        UserName=username
    )
    
    iam.create_login_profile(
        UserName=username,
        Password='Lato17',
        PasswordResetRequired=True
    )
    
    ak=iam.create_access_key(
        UserName=username
    )
    
    print(ak)
    
    iam.attach_user_policy(
        UserName='Pawel',
        PolicyArn='arn:aws:iam::050452088192:policy/DevAccess'
    )


createUser('Pawel')
createUser('Wojtek')
print ("cześć")
print ("Mikolaj z locala mowi cześć2")
#iam.detach_user_policy(
#    UserName='Pawel',
#    PolicyArn='arn:aws:iam::050452088192:policy/DevAccess'
#)
#
#iam.delete_policy(
#    PolicyArn='arn:aws:iam::050452088192:policy/DevAccess'
#)
