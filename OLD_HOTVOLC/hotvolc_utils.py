#!/usr/bin/python
# -*-coding:Utf-8 -*

import math

import numpy
from PIL import Image
from mpop.projector import get_area_def
from osgeo import osr
from pyproj import Proj


# ***************************************************************************************************************

def imagesc(data, color_path, color):
    """imagesc(data, color)
    data -> numpy array with data
    color path -> where are the colorbar
    color -> name of the color bar"""

    if color == 'temperatures_bleu_clair_fonce':
        # cas particulier de la colorbar fixe temperature de nuages
        R = numpy.ma.empty_like(data)
        G = numpy.ma.empty_like(data)
        B = numpy.ma.empty_like(data)
        alpha = numpy.ma.empty_like(data)

        R[numpy.ma.where(data < -80.0 + 273.15)] = 255
        G[numpy.ma.where(data < -80.0 + 273.15)] = 255
        B[numpy.ma.where(data < -80.0 + 273.15)] = 255
        alpha[numpy.ma.where(data < -80.0 + 273.15)] = 255

        R[numpy.ma.where(data >= -80.0 + 273.15)] = 255
        G[numpy.ma.where(data >= -80.0 + 273.15)] = 0
        B[numpy.ma.where(data >= -80.0 + 273.15)] = 0
        alpha[numpy.ma.where(data >= -80.0 + 273.15)] = 255

        R[numpy.ma.where(data >= -63.0 + 273.15)] = 255
        G[numpy.ma.where(data >= -63.0 + 273.15)] = 128
        B[numpy.ma.where(data >= -63.0 + 273.15)] = 0
        alpha[numpy.ma.where(data >= -63.0 + 273.15)] = 255

        R[numpy.ma.where(data >= -47.0 + 273.15)] = 247
        G[numpy.ma.where(data >= -47.0 + 273.15)] = 255
        B[numpy.ma.where(data >= -47.0 + 273.15)] = 20
        alpha[numpy.ma.where(data >= -47.0 + 273.15)] = 255

        R[numpy.ma.where(data >= -30.0 + 273.15)] = 0
        G[numpy.ma.where(data >= -30.0 + 273.15)] = 255
        B[numpy.ma.where(data >= -30.0 + 273.15)] = 0
        alpha[numpy.ma.where(data >= -30.0 + 273.15)] = 255

        R[numpy.ma.where(data >= -22.0 + 273.15)] = 15
        G[numpy.ma.where(data >= -22.0 + 273.15)] = 60
        B[numpy.ma.where(data >= -22.0 + 273.15)] = 0
        alpha[numpy.ma.where(data >= -22.0 + 273.15)] = 255

        R[numpy.ma.where(data >= -14.0 + 273.15)] = 0
        G[numpy.ma.where(data >= -14.0 + 273.15)] = 255
        B[numpy.ma.where(data >= -14.0 + 273.15)] = 255
        alpha[numpy.ma.where(data >= -47.0 + 273.15)] = 255

        R[numpy.ma.where(data >= -6.0 + 273.15)] = 0
        G[numpy.ma.where(data >= -6.0 + 273.15)] = 162
        B[numpy.ma.where(data >= -6.0 + 273.15)] = 245
        alpha[numpy.ma.where(data >= -6.0 + 273.15)] = 255
    else:
        colorbar_data = Image.open(color_path + color + '.png', 'r')
        colorbar_data = colorbar_data.getdata()

        # rÃ©cupÃ©ration des min/max
        min_data = numpy.ma.min(data)
        max_data = numpy.ma.max(data)

        # rÃ©-Ã©talement de la dynamique
        if min_data < max_data:
            index = numpy.around((((data - min_data) / (max_data - min_data)) * (colorbar_data.size[1] - 1)),
                                 decimals=0)
            index = index.astype(int)
        else:
            index = data * 0.0
            index = index.astype(int)

        # initialisation des R, G, B et alpha
        R = numpy.ma.empty_like(index)
        G = numpy.ma.empty_like(index)
        B = numpy.ma.empty_like(index)
        alpha = numpy.ma.empty_like(index)

        for c in range(colorbar_data.size[1]):
            R[numpy.ma.where(index == c)] = colorbar_data[c][0]
            G[numpy.ma.where(index == c)] = colorbar_data[c][1]
            B[numpy.ma.where(index == c)] = colorbar_data[c][2]
            alpha[numpy.ma.where(index == c)] = colorbar_data[c][3]

    R = numpy.ma.masked_where(numpy.ma.getmask(data) == True, R)
    G = numpy.ma.masked_where(numpy.ma.getmask(data) == True, G)
    B = numpy.ma.masked_where(numpy.ma.getmask(data) == True, B)
    alpha = numpy.ma.masked_where(numpy.ma.getmask(data) == True, alpha)

    # regroupe R, G et B dans un tuple
    RGB = []
    RGB.append(R)
    RGB.append(G)
    RGB.append(B)
    RGB.append(alpha)

    return RGB


# ***************************************************************************************************************

def parm_proj(area):
    """Fonction qui calcul les paramÃ¨tres de projection du geotiff"""

    area_mp = get_area_def(area)
    geotrf = [area_mp.area_extent[0], area_mp.pixel_size_x, 0,
              area_mp.area_extent[3], 0, -area_mp.pixel_size_y]
    srs = osr.SpatialReference()
    srs.ImportFromProj4(area_mp.proj4_string)
    srs.SetProjCS(area_mp.proj_id)
    try:
        srs.SetWellKnownGeogCS(area_mp.proj_dict['ellps'])
    except KeyError:
        pass
    try:
        srs.SetAuthority('PROJCS', 'EPSG',
                         int(area_mp.proj_dict['init'].split('espg')[1]))
    except (KeyError, IndexError):
        pass
    srs = srs.ExportToWkt()

    parm = []
    parm.append(geotrf)
    parm.append(srs)

    return parm


# ***************************************************************************************************************

def solarZenithAngle(dd, mm, HH, MM, lat, lon):
    """Fonction de calcul de l'angle solaire zenithal"""

    dayOffset = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334]
    dayOfYear = dd + dayOffset[mm - 1]

    gamma = (2 * math.pi / 365) * ((dayOfYear - 1) + (HH - 12) / 24)

    eqtime = 229.18 * (0.000075
                       + 0.001868 * math.cos(math.radians(gamma)) - 0.032077 * math.sin(math.radians(gamma))
                       - 0.014615 * math.cos(math.radians(2 * gamma)) - 0.040849 * math.sin(math.radians(2 * gamma)))

    declination = 0.006918 - (0.399912 * math.cos(math.radians(gamma))) + \
                  0.070257 * math.sin(math.radians(gamma)) - \
                  0.006758 * math.cos(math.radians(2 * gamma)) + \
                  0.000907 * math.sin(math.radians(2 * gamma)) - \
                  0.002697 * math.cos(math.radians(3 * gamma)) + \
                  0.00148 * math.sin(math.radians(3 * gamma))

    timeOffset = eqtime - 4 * lon
    trueSolarTime = HH * 60 + MM + timeOffset
    solarHourAngle = (trueSolarTime / 4) - 180

    solarZenithAngle = math.degrees(
        math.acos(((math.sin(math.radians(lat)) * math.sin(declination)) +
                   (math.cos(math.radians(lat)) * math.cos(declination) *
                    math.cos(math.radians(solarHourAngle))))))

    return solarZenithAngle


# ***************************************************************************************************************

def distOnSphere(lat_point1, lon_point1, lat_point2, lon_point2):
    """Fonction qui calcul la distance en mÃ¨tre entre deux points sur une sphÃ¨re"""

    r = 6371e3

    dLambda = lat_point1 - lat_point2
    distance = r * numpy.arccos((numpy.sin(numpy.radians(lon_point1)) * numpy.sin(numpy.radians(lon_point2))
                                 + (numpy.cos(numpy.radians(lon_point1)) * numpy.cos(numpy.radians(lon_point2))
                                    * numpy.cos(numpy.radians(dLambda)))))

    return distance


# ***************************************************************************************************************

def aeCalcMerc(lonInf, lonSup, latInf, latSup):
    """ """

    cmd = '+proj=merc +lon_0=0 +lat_0=0 +k=1 +x_0=0 +y_0=0 +a=6378137 +b=6378137 + towgs84=0,0,0,0,0,0,0 +units=m +no_defs'
    prj = Proj(cmd)
    lon_bbox = (lonInf, lonSup)
    lat_bbox = (latInf, latSup)
    x, y = prj(lon_bbox, lat_bbox)
    ae = (x[0], y[0], x[1], y[1])
    return ae


# ***************************************************************************************************************

def aeCalcGeos(lonInf, lonSup, latInf, latSup):
    """Fonction qui permet de calculer les distances entre le nadir d'un satellite
 geostationnaire et les coins infÃ©rieure gauche et supÃ©rieur droit d'une zone
 d'extraction"""

    prj = Proj('+proj=geos +lon_0 +h=35785831.0')
    lon_bbox = (lonInf, lonSup)
    lat_bbox = (latInf, latSup)
    x, y = prj(lon_bbox, lat_bbox)
    ae = (x[0], y[0], x[1], y[1])
    return ae
