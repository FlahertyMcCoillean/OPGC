#!/usr/bin/python
# -*-coding:Utf-8 -*

import hotvolc_conversions
import hotvolc_utils
import numpy


def NTI(Tb039, Tb120, cutoff, lat, lon, volcans):
    """Algo NTI"""
    # conversion en radiance spectrale
    rad039 = hotvolc_conversions.Tb2RadSpe(Tb039, 4)
    rad120 = hotvolc_conversions.Tb2RadSpe(Tb120, 10)
    NTI_raster = rad039
    NTI_flag = numpy.ma.masked_all_like(NTI_raster)

    lat = numpy.round(lat, 4)
    lon = numpy.round(lon, 4)

    NTI_monitoring = []
    NTI_volcan_monitoring = []
    Lava_Volume = []
    NTI_anomalies = []

    # Parcours de la liste des volcans
    for volcan in volcans:
        lat_volc = volcan['lat']
        lon_volc = volcan['lon']
        offN = volcan['offsetN']
        offS = volcan['offsetS']
        offE = volcan['offsetE']
        offW = volcan['offsetW']
        offnvN = volcan['offsetnvN']
        offnvS = volcan['offsetnvS']
        offnvE = volcan['offsetnvE']
        offnvW = volcan['offsetnvW']
        booleanVolume = volcan['volume_calculation']
        pix_area = volcan['pix_area']
        Tamb = volcan['Tamb']
        Tsurf = volcan['Tsurf']
        h = volcan['h']

        # Calcul de la position du volcan sur l'image
        dist = hotvolc_utils.distOnSphere(lat, lon, lat_volc, lon_volc)
        idx = numpy.where(dist == numpy.min(dist))

        # RÃ©cupÃ©ration de la zone d'interet
        lat_ROI = lat[idx[0][0] - offN:idx[0][0] + offS + 1, idx[1][0] - offW:idx[1][0] + offE + 1]
        lon_ROI = lon[idx[0][0] - offN:idx[0][0] + offS + 1, idx[1][0] - offW:idx[1][0] + offE + 1]
        rad039ROI = rad039[idx[0][0] - offN:idx[0][0] + offS + 1, idx[1][0] - offW:idx[1][0] + offE + 1]
        rad120ROI = rad120[idx[0][0] - offN:idx[0][0] + offS + 1, idx[1][0] - offW:idx[1][0] + offE + 1]
        NTI_ROI = 1 - numpy.absolute(((rad039ROI - rad120ROI) / (rad039ROI + rad120ROI)))

        # Recuperation de la zone non volcanique
        rad039_NVZ = rad039[idx[0][0] - (offN + offnvN):idx[0][0] + (offS + offnvS) + 1,
                     idx[1][0] - (offW + offnvW):idx[1][0] + (offE + offnvE) + 1]
        rad120_NVZ = rad120[idx[0][0] - (offN + offnvN):idx[0][0] + (offS + offnvS) + 1,
                     idx[1][0] - (offW + offnvW):idx[1][0] + (offE + offnvE) + 1]
        NTI_NVZ = 1 - numpy.absolute(((rad039_NVZ - rad120_NVZ) / (rad039_NVZ + rad120_NVZ)))
        xxx = NTI_NVZ.shape
        NTI_NVZ[(xxx[0] / 2) - offN:(xxx[0] / 2) + offS + 1, (xxx[1] / 2) - offW:(xxx[1] / 2) + offE + 1] = 999
        NTI_NVZ = numpy.ma.masked_where(NTI_NVZ == 999, NTI_NVZ)
        nti_cutoff = numpy.ma.max(NTI_NVZ) * cutoff

        i = 0
        for index, pix in numpy.ndenumerate(NTI_ROI):
            NTI_volcan_monitoring_buffer = {}
            NTI_anomalies_buffer = {}
            NTI_volcan_monitoring_buffer['volcan'] = volcan['volcan']
            NTI_volcan_monitoring_buffer['pixID'] = i
            NTI_volcan_monitoring_buffer['nti'] = pix
            i += 1
            if pix > nti_cutoff:
                NTI_volcan_monitoring_buffer['anomaly'] = True
                NTI_anomalies_buffer['volcan'] = volcan['volcan']
                NTI_anomalies_buffer['lat'] = lat_ROI[index[0]][index[1]]
                NTI_anomalies_buffer['lon'] = lon_ROI[index[0]][index[1]]
                NTI_anomalies_buffer['r39'] = rad039ROI[index[0]][index[1]]
                NTI_anomalies_buffer['r12'] = rad120ROI[index[0]][index[1]]
                NTI_anomalies_buffer['nti'] = pix
                NTI_anomalies.append(NTI_anomalies_buffer)
            else:
                NTI_volcan_monitoring_buffer['anomaly'] = False

            NTI_volcan_monitoring.append(NTI_volcan_monitoring_buffer)

        NTI_ROI = numpy.ma.masked_where(NTI_ROI < nti_cutoff, NTI_ROI)
        NTI_flag[idx[0][0] - offN:idx[0][0] + offS + 1, idx[1][0] - offW:idx[1][0] + offE + 1] = NTI_ROI
        TSR_temp = numpy.ma.masked_where(NTI_ROI < nti_cutoff, rad039ROI)

        if booleanVolume == True:
            rad039_anomalies_mean = numpy.ma.mean(TSR_temp)
            Lamb = hotvolc_conversions.Tb2RadSpe(Tamb, 4)
            Lsurf = hotvolc_conversions.Tb2RadSpe(Tsurf, 4)
            activeLavaFraction = (rad039_anomalies_mean - Lamb) / (Lsurf - Lamb)
            total_pix_area = (pix_area * NTI_ROI.count()) * 1e6
            volume_instantaneous = activeLavaFraction * total_pix_area * h
            Lava_Volume_buffer = {}
            Lava_Volume_buffer['volcan'] = volcan['volcan']
            if NTI_ROI.count() > 0:
                Lava_Volume_buffer['volume'] = volume_instantaneous
            else:
                Lava_Volume_buffer['volume'] = 0.0
            Lava_Volume_buffer['Tamb'] = Tamb
            Lava_Volume_buffer['Tsurf'] = Tsurf
            Lava_Volume_buffer['h'] = h

            Lava_Volume.append(Lava_Volume_buffer)

        # RÃ©cupÃ©ration des donnÃ©es via un dictionnaire
        NTI_monitoring_buffer = {}
        NTI_monitoring_buffer['volcan'] = volcan['volcan']
        NTI_monitoring_buffer['sizeROI'] = rad039ROI.size
        NTI_monitoring_buffer['nbAnomalies'] = NTI_ROI.count()
        if NTI_ROI.count() > 0:
            NTI_monitoring_buffer['TSR'] = numpy.ma.sum(TSR_temp)
        else:
            NTI_monitoring_buffer['TSR'] = 0.0
        if rad039ROI.count() > 0:
            NTI_monitoring_buffer['r39mean'] = numpy.mean(rad039ROI)
            NTI_monitoring_buffer['r39max'] = numpy.max(rad039ROI)
            NTI_monitoring_buffer['r39min'] = numpy.min(rad039ROI)
        else:
            NTI_monitoring_buffer['r39mean'] = 0.0
            NTI_monitoring_buffer['r39max'] = 0.0
            NTI_monitoring_buffer['r39min'] = 0.0
        if rad120ROI.count() > 0:
            NTI_monitoring_buffer['r12mean'] = numpy.mean(rad120ROI)
            NTI_monitoring_buffer['r12max'] = numpy.max(rad120ROI)
            NTI_monitoring_buffer['r12min'] = numpy.min(rad120ROI)
        else:
            NTI_monitoring_buffer['r12mean'] = 0.0
            NTI_monitoring_buffer['r12max'] = 0.0
            NTI_monitoring_buffer['r12min'] = 0.0

        NTI_monitoring.append(NTI_monitoring_buffer)

    NTI_products = []
    NTI_products.append(NTI_raster)
    NTI_products.append(NTI_flag)
    NTI_products.append(NTI_monitoring)
    NTI_products.append(NTI_volcan_monitoring)
    NTI_products.append(NTI_anomalies)
    NTI_products.append(Lava_Volume)

    return NTI_products


def ASH2(Tb108, Tb120, cutoff1):
    """Algo ash - Prata"""
    BTD = Tb108 - Tb120
    BTD = numpy.ma.masked_where(BTD > cutoff1, BTD)
    tmp = numpy.ma.masked_where(BTD > cutoff1, Tb108)

    ASH = []
    ASH.append(BTD)
    ASH.append(tmp)

    return ASH


def ASH3(Tb087, Tb108, Tb120, cutoff1, cutoff2):
    """Algo ash - 3 bandes"""
    BTD1 = Tb108 - Tb120
    BTD2 = Tb087 - Tb108

    BTD1 = numpy.ma.masked_where(BTD1 > cutoff1, BTD1)
    BTD1 = numpy.ma.masked_where(BTD2 < cutoff2, BTD1)

    tmp = numpy.ma.masked_where(BTD1 > cutoff1, Tb108)
    tmp = numpy.ma.masked_where(BTD2 < cutoff2, Tb108)

    ASH = []
    ASH.append(BTD1)
    ASH.append(tmp)

    return ASH


def Cloud(Tb108):
    """Algo cloud coverage"""
    cloud_raster = numpy.ma.masked_where(Tb108 > 274.15, Tb108)
    cloud_cover_raster = numpy.ma.masked_where(Tb108 > 259.0, Tb108)
    if cloud_raster.count() > 0:
        cloudCOVER = ((cloud_cover_raster.count() * 1.0) / (cloud_cover_raster.size * 1.0)) * 100.0
    else:
        cloudCOVER = 0.0
    cloud = []
    cloud.append(cloud_raster)
    cloud.append(cloudCOVER)

    return cloud


def SO2_LA(Tb087, Tb108, cutoff):
    """Algo SO2 basse altitude"""
    rad087 = hotvolc_conversions.Tb2RadSpe(Tb087, 7)
    rad108 = hotvolc_conversions.Tb2RadSpe(Tb108, 9)

    RSD = rad108 - rad087
    RSD = numpy.ma.masked_where(RSD < cutoff, RSD)

    return RSD


def SO2_HA(Tb073, Tb108, cutoff):
    """Algo SO2 haute altitude"""
    rad073 = hotvolc_conversions.Tb2RadSpe(Tb073, 6)
    rad108 = hotvolc_conversions.Tb2RadSpe(Tb108, 9)

    RSD = rad108 - rad073
    RSD = numpy.ma.masked_where(RSD < cutoff, RSD)

    return RSD


def quality(lat, lon, Tb108, volcans):
    """Algo quality flag"""
    quality_all = []

    for volcan in volcans:
        lat_volc = volcan['lat']
        lon_volc = volcan['lon']
        offN = volcan['offsetN']
        offS = volcan['offsetS']
        offE = volcan['offsetE']
        offW = volcan['offsetW']

        # calcul de la position du volcan sur l'image
        lat = numpy.round(lat, 4)
        lon = numpy.round(lon, 4)

        dist = hotvolc_utils.distOnSphere(lat, lon, lat_volc, lon_volc)
        idx = numpy.where(dist == numpy.min(dist))

        tb_ROI = Tb108[idx[0][0] - offN:idx[0][0] + offS + 1, idx[1][0] - offW:idx[1][0] + offE + 1]

        cloudcover_ROI = numpy.ma.masked_where(tb_ROI > 273.0, tb_ROI)
        if cloudcover_ROI.count() > 0:
            tmp_cloud_cover = ((cloudcover_ROI.count() * 1.0) / (cloudcover_ROI.size * 1.0)) * 100.0
        else:
            tmp_cloud_cover = 0.0

        Tb108_mean = numpy.ma.mean(tb_ROI)

        if Tb108_mean < 243.0 and tmp_cloud_cover > 0.0:
            quality_flag = 0
        elif Tb108_mean <= 251.0 and Tb108_mean > 243.0 and tmp_cloud_cover > 50.0:
            quality_flag = 1
        elif Tb108_mean <= 251.0 and Tb108_mean > 243.0 and tmp_cloud_cover < 50.0:
            quality_flag = 2
        elif Tb108_mean <= 259 and Tb108_mean > 251 and tmp_cloud_cover > 50:
            quality_flag = 2
        elif Tb108_mean <= 259 and Tb108_mean > 251 and tmp_cloud_cover < 50:
            quality_flag = 3
        elif Tb108_mean <= 267 and Tb108_mean > 259 and tmp_cloud_cover > 50:
            quality_flag = 3
        elif Tb108_mean <= 267 and Tb108_mean > 259 and tmp_cloud_cover < 50:
            quality_flag = 4
        elif Tb108_mean <= 273 and Tb108_mean > 267 and tmp_cloud_cover > 0:
            quality_flag = 4
        elif Tb108_mean <= 273 and Tb108_mean > 267 and tmp_cloud_cover == 0:
            quality_flag = 5
        elif Tb108_mean > 273:
            quality_flag = 5
        else:
            quality_flag = 0

        quality_buffer = {}
        quality_buffer['volcan'] = volcan['volcan']
        quality_buffer['Tb108_mean'] = Tb108_mean
        quality_buffer['Cloud_Cover'] = tmp_cloud_cover
        quality_buffer['Quality_Flag'] = quality_flag

        quality_all.append(quality_buffer)

    return quality_all
