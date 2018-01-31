# -*- coding: utf-8 -*-
"""
Created on Tue Aug 22 20:37:37 2017

@author: Mikolaj
"""

import numpy as np

########## RGB ################
def getRGBFromPic(picture):
    
    R, G, B = picture.split()
    r_mean=np.mean(R.getdata())
    g_mean=np.mean(G.getdata())
    b_mean=np.mean(B.getdata())
    
    return [r_mean,g_mean,b_mean]

########## HSV ###############
def getHue(r,g,b,ma,mi):
    if ma==mi:
        return 0
    elif r==ma:
        hue= (g-b)/(ma-mi)
    elif g==ma:
        hue= 2.0 + (b-r)/(ma-mi)
    else:
        hue= 4.0 + (r-g)/(ma-mi)
    if hue<0:
        return hue*60+360
    else:
        return hue*60

def getSat(ma,mi):
    if ma==0:
        return 0
    else: 
        return (ma-mi)/ma

def getVal(ma):
    return ma  

def RGB2HSV(pixel,picture):
    cell=picture.getdata()[pixel]
    
    r=cell[0]/255
    g=cell[1]/255
    b=cell[2]/255
    ma=max(r,g,b)      
    mi=min(r,g,b)
        
    return [getHue(r,g,b,ma,mi), getSat(ma,mi), getVal(ma)] 

def getHSVFromPic(picture):

    img=[]
    for pixel in range(picture.size[0]*picture.size[1]):
        img.append(RGB2HSV(pixel,picture))
        
    return list(np.array(img).mean(axis=0)) 

#################### ANALIZA ZDJECIA (RGB +HSV ) #########

def analyzeOnePic(picture):
    return getRGBFromPic(picture)+getHSVFromPic(picture)  
