#!/usr/bin/python
# -*- coding: utf-8 -*-

import datetime
import os
import sys
import tarfile
from subprocess import Popen

import hotvolc_algos
import hotvolc_conversions
import hotvolc_database
import hotvolc_readRaw
import hotvolc_utils
import numpy
import psycopg2
from mpop.imageo.geo_image import GeoImage
from mpop.projector import get_area_def
from mpop.satellites import GeostationaryFactory


def hotvolc_main(zone, yyyy, mm, dd, HH, MM, MSG=1, raw=False):
    """ Processing des produits pour 1 zone et pour 1 date """

    if zone == 'met09globeFull':

        con = hotvolc_database.DB_connection()
        try:
            with con:
                with con.cursor("path") as cur:
                    cur.execute("SELECT * FROM hotvolc_path;")
                    for buff in cur:
                        path_xrit = buff[1]
                        path_raw = buff[2]
                        path_product = buff[3]
                        path_colorbar = buff[4]
        except psycopg2.DatabaseError, e:
            if con:
                con.rollback()
                hotvolc_database.DB_close(con)
            print
            'Error %s' % e
            sys.exit(1)
        hotvolc_database.DB_close(con)

        path_final_product = '%s%d%02d%02d/%02d%02d/%s/' % (path_product,
                                                            yyyy, mm, dd,
                                                            HH, MM, zone)
        try:
            os.makedirs(path_final_product, mode=0777)
        except OSError:
            if not os.path.isdir(path_final_product):
                raise

        data_area = get_area_def(zone)
        time_slot_fake = datetime.datetime(2014, 12, 14, 14, 30)
        time_slot = datetime.datetime(yyyy, mm, dd, HH, MM)
        data = GeostationaryFactory.create_scene('meteosat',
                                                 '09',
                                                 'seviri',
                                                 time_slot_fake)
        data.load([3.9, 8.7, 10.8, 12.0],
                  area_extent=data_area.area_extent,
                  calibrate=0)

        data_108 = hotvolc_readRaw.readFullMSGRaw(path_raw, 9, yyyy, mm, dd, HH, MM)

        data[10.8].data = data_108
        data[10.8].data = numpy.ma.masked_where(data[10.8].data < 0, data[10.8].data)

        if data[10.8].data.count() == 0:
            print
            'donnees manquantes - traitement annule'
            return

        data[10.8].data = hotvolc_conversions.CN2RadEff(data[10.8].data, 9,
                                                        path_raw, yyyy, mm, dd, HH, MM)
        data[10.8].data = hotvolc_conversions.RadEff2Tb(data[10.8].data, 9)

        data[10.8].data[numpy.ma.where(data[10.8].data < 163.0)] = 163.0

        Cloud_raster, Cloud_cover = hotvolc_algos.Cloud(data[10.8].data)

        CloudRGB_red, CloudRGB_green, CloudRGB_blue, CloudRGB_alpha = \
            hotvolc_utils.imagesc(Cloud_raster, path_colorbar, 'temperatures_bleu_clair_fonce')
        CloudGRAY_red, CloudGRAY_green, CloudGRAY_blue, CloudGRAY_alpha = \
            hotvolc_utils.imagesc(Cloud_raster, path_colorbar, 'cloud_white')

        geotrf, srs = hotvolc_utils.parm_proj(zone)

        data[3.9].data = CloudRGB_red
        data[8.7].data = CloudRGB_green
        data[10.8].data = CloudRGB_blue
        data[12.0].data = CloudRGB_alpha

        img_CloudRGB_path = '%s/%d%02d%02d%02d%02d_%s_GLOBE_RGB.tif' % (path_final_product, yyyy, mm, dd, HH, MM, zone)
        img = GeoImage((data[3.9].data,
                        data[8.7].data,
                        data[10.8].data,
                        data[12.0].data),
                       zone,
                       time_slot,
                       mode='RGBA')
        img.enhance(stretch='crude', gamma=1)
        img.geotiff_save(img_CloudRGB_path,
                         compression=0,
                         geotransform=geotrf,
                         spatialref=srs)

        data[3.9].data = CloudGRAY_red
        data[8.7].data = CloudGRAY_green
        data[10.8].data = CloudGRAY_blue
        data[12.0].data = CloudGRAY_alpha

        img_CloudGRAY_path = '%s/%d%02d%02d%02d%02d_%s_GLOBE_GRAY.tif' % (
        path_final_product, yyyy, mm, dd, HH, MM, zone)
        img = GeoImage((data[3.9].data,
                        data[8.7].data,
                        data[10.8].data,
                        data[12.0].data),
                       zone,
                       time_slot,
                       mode='RGBA')
        img.enhance(stretch='crude', gamma=1)
        img.geotiff_save(img_CloudGRAY_path,
                         compression=0,
                         geotransform=geotrf,
                         spatialref=srs)

        img_CloudRGB_reproj_path = '%s/%d%02d%02d%02d%02d_%s_GLOBE_RGB_reproj.tif' % \
                                   (path_final_product, yyyy, mm, dd, HH, MM, zone)
        img_CloudGRAY_reproj_path = '%s/%d%02d%02d%02d%02d_%s_GLOBE_GRAY_reproj.tif' % \
                                    (path_final_product, yyyy, mm, dd, HH, MM, zone)

        def reproj_tiff(img_in, img_out):
            """ Reprojection des geotiff de satellite vers mercator"""
            sys.argv = ['gdalwarp', '-t_srs', '+proj=merc +ellps=WGS84', img_in, img_out]
            p = Popen(sys.argv, close_fds=True)
            return p

        reproj_tiff(img_CloudRGB_path, img_CloudRGB_reproj_path)
        last_proj = reproj_tiff(img_CloudGRAY_path, img_CloudGRAY_reproj_path)
        last_proj.wait()

        def create_tiles_zoom5(image_product, product):
            """ creation des pyramides avec gdal2tiles.py
            retourne object Popen
            :rtype : object """
            GDAL_EXEC = '/usr/bin/gdal2tiles.py'
            sys.argv = [GDAL_EXEC, '-r', 'near', '-z0-5', image_product]
            sys.argv.append('{0}/{1}'.format(path_final_product, product))
            p = Popen(sys.argv, close_fds=True)
            return p

        create_tiles_zoom5(img_CloudRGB_reproj_path, 'GLOBE_RGB')
        last_tiles = create_tiles_zoom5(img_CloudGRAY_reproj_path, 'GLOBE_GRAY')
        last_tiles.wait()

        files_to_delete = []
        files_to_delete.append(img_CloudRGB_path)
        files_to_delete.append(img_CloudRGB_reproj_path)
        files_to_delete.append(img_CloudGRAY_path)
        files_to_delete.append(img_CloudGRAY_reproj_path)

        for name in files_to_delete:
            if os.path.exists(name):
                try:
                    os.remove(name)
                except OSError, e:
                    print
                    'Error: %s - %s' % (e.filename, e.strerror)

    else:
        zone_sat = zone + '_sp'
        zone_merc = zone + '_mp'
        list_MSG = ['08', '09', '10', '11']
        volcans = []

        # **** RÃ©cup des params dans la BDD ****
        con = hotvolc_database.DB_connection()
        try:
            # **** RÃ©cupÃ©ration des param extraction et cutoff ****
            with con:
                with con.cursor("param") as cur:
                    SQL = "SELECT x_min, x_max, y_min, y_max, ash_cutoff1, ash_cutoff2,\
                     nti_cutoff_day, nti_cutoff_night, so2_la_cutoff, so2_ha_cutoff \
                     FROM hotvolc_zones WHERE zone = (%s);"
                    DATA = (zone,)
                    cur.execute(SQL, DATA)
                    for buff in cur:
                        xmin = buff[0]
                        xmax = buff[1]
                        ymin = buff[2]
                        ymax = buff[3]
                        ash_cutoff1 = buff[4]
                        ash_cutoff2 = buff[5]
                        nti_cutoff_day = buff[6]
                        nti_cutoff_night = buff[7]
                        so2_la_cutoff = buff[8]
                        so2_ha_cutoff = buff[9]

            # **** RÃ©cupÃ©ration file_path ****
            with con:
                with con.cursor("path") as cur:
                    cur.execute("SELECT * FROM hotvolc_path;")
                    for buff in cur:
                        path_xrit = buff[1]
                        path_raw = buff[2]
                        path_product = buff[3]
                        path_colorbar = buff[4]

            # **** RÃ©cupÃ©ration des diffÃ©rents volcans sur zone ****
            with con:
                with con.cursor("volcan") as cur:
                    SQL = "SELECT * FROM hotvolc_volcanoes_monitoring WHERE zone = (%s);"
                    DATA = (zone,)
                    cur.execute(SQL, DATA)
                    for buff in cur:
                        tmp = {}
                        tmp['volcan'] = buff[2]
                        tmp['lat'] = buff[3]
                        tmp['lon'] = buff[4]
                        tmp['offsetN'] = buff[5]
                        tmp['offsetS'] = buff[6]
                        tmp['offsetE'] = buff[7]
                        tmp['offsetW'] = buff[8]
                        tmp['offsetnvN'] = buff[9]
                        tmp['offsetnvS'] = buff[10]
                        tmp['offsetnvE'] = buff[11]
                        tmp['offsetnvW'] = buff[12]
                        tmp['offsetN_atmos'] = buff[13]
                        tmp['offsetS_atmos'] = buff[14]
                        tmp['offsetE_atmos'] = buff[15]
                        tmp['offsetW_atmos'] = buff[16]
                        tmp['volume_calculation'] = buff[17]
                        tmp['pix_area'] = buff[18]
                        tmp['Tamb'] = buff[19]
                        tmp['Tsurf'] = buff[20]
                        tmp['h'] = buff[21]
                        volcans.append(tmp)

        except psycopg2.DatabaseError, e:
            if con:
                con.rollback()
                hotvolc_database.DB_close(con)
            print
            'Error %s' % e
            sys.exit(1)
        hotvolc_database.DB_close(con)

        # **** Lecture params zone via mpop ****
        data_area = get_area_def(zone_sat)

        # **** Mise en forme de la date ****
        time_slot = datetime.datetime(yyyy, mm, dd, HH, MM)

        # **** Lecture des donnÃ©es si Raw ****
        if raw:
            time_slot_fake = datetime.datetime(2014, 12, 14, 14, 30)
            data = GeostationaryFactory.create_scene("meteosat", "09", "seviri", time_slot_fake)
            data.load([3.9, 7.3, 8.7, 10.8, 12.0],
                      area_extent=data_area.area_extent,
                      calibrate=0)
            # print data

            data_039 = hotvolc_readRaw.readRoiMSGRaw(path_raw, 4, xmax, xmin,
                                                     ymax, ymin,
                                                     yyyy, mm, dd, HH, MM)
            data_073 = hotvolc_readRaw.readRoiMSGRaw(path_raw, 6, xmax, xmin,
                                                     ymax, ymin,
                                                     yyyy, mm, dd, HH, MM)
            data_087 = hotvolc_readRaw.readRoiMSGRaw(path_raw, 7, xmax, xmin,
                                                     ymax, ymin,
                                                     yyyy, mm, dd, HH, MM)
            data_108 = hotvolc_readRaw.readRoiMSGRaw(path_raw, 9, xmax, xmin,
                                                     ymax, ymin,
                                                     yyyy, mm, dd, HH, MM)
            data_120 = hotvolc_readRaw.readRoiMSGRaw(path_raw, 10, xmax, xmin,
                                                     ymax, ymin,
                                                     yyyy, mm, dd, HH, MM)

            data[3.9].data = data_039
            data[7.3].data = data_073
            data[8.7].data = data_087
            data[10.8].data = data_108
            data[12.0].data = data_120

        # **** Lecture des donnÃ©es fichiers XRIT ****
        else:
            data = GeostationaryFactory.create_scene('meteosat',
                                                     '09',
                                                     'seviri',
                                                     time_slot)
            data.load([3.9, 7.3, 8.7, 10.8, 12.0],
                      area_extent=data_area.area_extent,
                      calibrate=0)

        # **** CrÃ©ation du dossier oÃ¹ seront crÃ©Ã©s les produits ****
        path_final_product = '%s%d%02d%02d/%02d%02d/%s/' % (path_product,
                                                            yyyy, mm, dd,
                                                            HH, MM, zone)
        try:
            os.makedirs(path_final_product, mode=0777)
        except OSError:
            if not os.path.isdir(path_final_product):
                raise

        # **** Gestion des donnÃ©es corrompues ou hors limites qui ont un CN nÃ©gatif ****
        data[3.9].data = numpy.ma.masked_where(data[3.9].data < 0, data[3.9].data)
        data[7.3].data = numpy.ma.masked_where(data[7.3].data < 0, data[7.3].data)
        data[8.7].data = numpy.ma.masked_where(data[8.7].data < 0, data[8.7].data)
        data[10.8].data = numpy.ma.masked_where(data[10.8].data < 0, data[10.8].data)
        data[12.0].data = numpy.ma.masked_where(data[12.0].data < 0, data[12.0].data)

        if data[3.9].data.count() == 0:
            print
            'donnÃ©es manquantes - traitement annulÃ©'
            return
        if data[7.3].data.count() == 0:
            print
            'donnÃ©es manquantes - traitement annulÃ©'
            return
        if data[8.7].data.count() == 0:
            print
            'donnÃ©es manquantes - traitement annulÃ©'
            return
        if data[10.8].data.count() == 0:
            print
            'donnÃ©es manquantes - traitement annulÃ©'
            return
        if data[12.0].data.count() == 0:
            print
            'donnÃ©es manquantes - traitement annulÃ©'
            return

        # **** Conversion du CN vers Radiance Effective ****
        data[3.9].data = hotvolc_conversions.CN2RadEff(data[3.9].data, 4,
                                                       path_raw, yyyy, mm, dd, HH, MM)
        data[7.3].data = hotvolc_conversions.CN2RadEff(data[7.3].data, 6,
                                                       path_raw, yyyy, mm, dd, HH, MM)
        data[8.7].data = hotvolc_conversions.CN2RadEff(data[8.7].data, 7,
                                                       path_raw, yyyy, mm, dd, HH, MM)
        data[10.8].data = hotvolc_conversions.CN2RadEff(data[10.8].data, 9,
                                                        path_raw, yyyy, mm, dd, HH, MM)
        data[12.0].data = hotvolc_conversions.CN2RadEff(data[12.0].data, 10,
                                                        path_raw, yyyy, mm, dd, HH, MM)

        # **** Conversion vers la temperature de brillance ****
        data[3.9].data = hotvolc_conversions.RadEff2Tb(data[3.9].data, 4)
        data[7.3].data = hotvolc_conversions.RadEff2Tb(data[7.3].data, 6)
        data[8.7].data = hotvolc_conversions.RadEff2Tb(data[8.7].data, 7)
        data[10.8].data = hotvolc_conversions.RadEff2Tb(data[10.8].data, 9)
        data[12.0].data = hotvolc_conversions.RadEff2Tb(data[12.0].data, 10)

        # **** 'Sauvegarde' temporaire des variables (just in case)****
        Tb_IR039 = data[3.9].data
        Tb_IR073 = data[7.3].data
        Tb_IR087 = data[8.7].data
        Tb_IR108 = data[10.8].data
        Tb_IR120 = data[12.0].data

        # **** RÃ©cupÃ©ration matrice lat/lon ****
        lon, lat = data[3.9].area.get_lonlats()

        # **** Calcul du Solar Zenith Angle au centre de l'image ****
        lat_center = lat[int(round(data[3.9].data.shape[0] / 2, 0)),
                         int(round(data[3.9].data.shape[1] / 2, 0))]
        lon_center = lon[int(round(data[3.9].data.shape[0] / 2, 0)),
                         int(round(data[3.9].data.shape[1] / 2, 0))]

        SZA = hotvolc_utils.solarZenithAngle(dd, mm, HH, MM, lat_center, lon_center)

        if SZA > 90.0:
            nti_cutoff = nti_cutoff_night
        else:
            nti_cutoff = nti_cutoff_day

            # **** crÃ©ation des produits ****
        ASH2_semiflag, Tb108_ASH2 = hotvolc_algos.ASH2(data[10.8].data,
                                                       data[12.0].data,
                                                       ash_cutoff1)

        ASH3_semiflag, Tb108_ASH3 = hotvolc_algos.ASH3(data[8.7].data,
                                                       data[10.8].data,
                                                       data[12.0].data,
                                                       ash_cutoff1,
                                                       ash_cutoff2)

        NTI_raster, NTI_flag, NTI_monitoring, NTI_monitoring_volcan, NTI_anomalies, Lava_Volume = hotvolc_algos.NTI(
            data[3.9].data,
            data[12.0].data,
            nti_cutoff,
            lat,
            lon,
            volcans)

        Cloud_raster, cloudCOVER = hotvolc_algos.Cloud(data[10.8].data)

        SO2_LA_semiflag = hotvolc_algos.SO2_LA(data[8.7].data,
                                               data[10.8].data,
                                               so2_la_cutoff)

        SO2_HA_semiflag = hotvolc_algos.SO2_HA(data[7.3].data,
                                               data[10.8].data,
                                               so2_ha_cutoff)

        quality_dic = hotvolc_algos.quality(lat, lon, data[10.8].data, volcans)

        # **** Passage en fausse couleur ****
        ASH2_semiflag_red, ASH2_semiflag_green, ASH2_semiflag_blue, AHS2_semiflag_alpha = \
            hotvolc_utils.imagesc(ASH2_semiflag, path_colorbar, "hotvolc_inverse")  # hotvolc_inverse
        ASH3_semiflag_red, ASH3_semiflag_green, ASH3_semiflag_blue, AHS3_semiflag_alpha = \
            hotvolc_utils.imagesc(ASH3_semiflag, path_colorbar, "hotvolc_inverse")  # hotvolc_inverse
        NTI_raster_red, NTI_raster_green, NTI_raster_blue, NTI_raster_alpha = \
            hotvolc_utils.imagesc(NTI_raster, path_colorbar, "hotvolc")  # hotvolc
        Cloud_raster_red, Cloud_raster_green, Cloud_raster_blue, Cloud_raster_alpha = \
            hotvolc_utils.imagesc(Cloud_raster, path_colorbar, "temperatures_bleu_clair_fonce")
        SO2_LA_semiflag_red, SO2_LA_semiflag_green, SO2_LA_semiflag_blue, SO2_LA_semiflag_alpha = \
            hotvolc_utils.imagesc(SO2_LA_semiflag, path_colorbar, "hotvolc")  # hotvolc
        SO2_HA_semiflag_red, SO2_HA_semiflag_green, SO2_HA_semiflag_blue, SO2_HA_semiflag_alpha = \
            hotvolc_utils.imagesc(SO2_HA_semiflag, path_colorbar, "hotvolc")  # hotvolc

        # **** Cas particulier du flag NTI ****
        NTI_flag_red = numpy.ma.empty_like(NTI_flag)
        NTI_flag_green = numpy.ma.empty_like(NTI_flag)
        NTI_flag_blue = numpy.ma.empty_like(NTI_flag)

        NTI_flag_red[numpy.ma.where(numpy.ma.getmask(NTI_flag) == False)] = 255
        NTI_flag_green[numpy.ma.where(numpy.ma.getmask(NTI_flag) == False)] = 0
        NTI_flag_blue[numpy.ma.where(numpy.ma.getmask(NTI_flag) == False)] = 0

        # **** RÃ©cup parmas reprojection mercator ****
        geotrf, srs = hotvolc_utils.parm_proj(zone_merc)

        # **** DÃ©but - CrÃ©ation gÃ©otiff ASH2 ****
        data[3.9].data = ASH2_semiflag_red
        data[8.7].data = ASH2_semiflag_green
        data[10.8].data = ASH2_semiflag_blue

        # *** Reprojection en mercator ***
        ASH2_semiflag_mercator = data.project(zone_merc)

        # *** CrÃ©ation du gÃ©otiff ***
        img_ASH2_semiflag_path = '%s/%d%02d%02d%02d%02d_%s_ASH2.tif' % (path_final_product, yyyy, mm, dd, HH, MM, zone)
        img_ASH2_semiflag = GeoImage((ASH2_semiflag_mercator[3.9].data,
                                      ASH2_semiflag_mercator[8.7].data,
                                      ASH2_semiflag_mercator[10.8].data),
                                     zone_merc,
                                     time_slot,
                                     mode="RGB")
        img_ASH2_semiflag.enhance(stretch="crude", gamma=1)
        img_ASH2_semiflag.geotiff_save(img_ASH2_semiflag_path,
                                       compression=0,
                                       geotransform=geotrf,
                                       spatialref=srs)
        # **** Fin - CrÃ©ation gÃ©otiff ASH2 ****

        # **** DÃ©but - CrÃ©ation gÃ©otiff ASH3 ****
        data[3.9].data = ASH3_semiflag_red
        data[8.7].data = ASH3_semiflag_green
        data[10.8].data = ASH3_semiflag_blue

        ASH3_semiflag_mercator = data.project(zone_merc)

        img_ASH3_semiflag_path = '%s/%d%02d%02d%02d%02d_%s_ASH3.tif' % (path_final_product, yyyy, mm, dd, HH, MM, zone)
        img_ASH3_semiflag = GeoImage((ASH3_semiflag_mercator[3.9].data,
                                      ASH3_semiflag_mercator[8.7].data,
                                      ASH3_semiflag_mercator[10.8].data),
                                     zone_merc,
                                     time_slot,
                                     mode="RGB")
        img_ASH3_semiflag.enhance(stretch="crude", gamma=1)
        img_ASH3_semiflag.geotiff_save(img_ASH3_semiflag_path,
                                       compression=0,
                                       geotransform=geotrf,
                                       spatialref=srs)
        # **** Fin - CrÃ©ation gÃ©otiff ASH3 ****

        # **** DÃ©but - CrÃ©ation gÃ©otiff Cloud ****
        data[3.9].data = Cloud_raster_red
        data[8.7].data = Cloud_raster_green
        data[10.8].data = Cloud_raster_blue
        data[12.0].data = Cloud_raster_alpha

        Cloud_raster_mercator = data.project(zone_merc)

        img_Cloud_raster_path = '%s/%d%02d%02d%02d%02d_%s_Cloud.tif' % (path_final_product, yyyy, mm, dd, HH, MM, zone)
        img_Cloud_raster = GeoImage((Cloud_raster_mercator[3.9].data,
                                     Cloud_raster_mercator[8.7].data,
                                     Cloud_raster_mercator[10.8].data,
                                     Cloud_raster_mercator[12.0].data),
                                    zone_merc,
                                    time_slot,
                                    mode="RGBA")
        img_Cloud_raster.enhance(stretch="crude", gamma=1)
        img_Cloud_raster.geotiff_save(img_Cloud_raster_path,
                                      compression=0,
                                      geotransform=geotrf,
                                      spatialref=srs)
        # **** Fin - CrÃ©ation gÃ©otiff Cloud ****

        # **** DÃ©but - CrÃ©ation gÃ©otiff NTI raster ****
        data[3.9].data = NTI_raster_red
        data[8.7].data = NTI_raster_green
        data[10.8].data = NTI_raster_blue

        NTI_raster_mercator = data.project(zone_merc)

        img_NTI_raster_path = '%s/%d%02d%02d%02d%02d_%s_NTI_raster.tif' % (
        path_final_product, yyyy, mm, dd, HH, MM, zone)
        img_NTI_raster = GeoImage((NTI_raster_mercator[3.9].data,
                                   NTI_raster_mercator[8.7].data,
                                   NTI_raster_mercator[10.8].data),
                                  zone_merc,
                                  time_slot,
                                  mode="RGB")
        img_NTI_raster.enhance(stretch="crude", gamma=1)
        img_NTI_raster.geotiff_save(img_NTI_raster_path,
                                    compression=0,
                                    geotransform=geotrf,
                                    spatialref=srs)
        # **** Fin - CrÃ©ation gÃ©otiff NTI raster ****

        # **** DÃ©but - CrÃ©ation gÃ©otiff NTI flag ****
        data[3.9].data = NTI_flag_red
        data[8.7].data = NTI_flag_green
        data[10.8].data = NTI_flag_blue

        NTI_flag_mercator = data.project(zone_merc)

        img_NTI_flag_path = '%s/%d%02d%02d%02d%02d_%s_NTI_flag.tif' % (path_final_product, yyyy, mm, dd, HH, MM, zone)
        img_NTI_flag = GeoImage((NTI_flag_mercator[3.9].data,
                                 NTI_flag_mercator[8.7].data,
                                 NTI_flag_mercator[10.8].data),
                                zone_merc,
                                time_slot,
                                mode="RGB")
        img_NTI_flag.enhance(stretch="crude", gamma=1)
        img_NTI_flag.geotiff_save(img_NTI_flag_path,
                                  compression=0,
                                  geotransform=geotrf,
                                  spatialref=srs)
        # **** Fin - CrÃ©ation gÃ©otiff NTI flag ****

        # **** DÃ©but - CrÃ©ation gÃ©otiff RGB raster ****
        data[8.7].data = Tb_IR087
        data[10.8].data = Tb_IR108
        data[12.0].data = Tb_IR120

        RGB_raster_merc = data.project(zone_merc)
        BTD1_RGB_raster = RGB_raster_merc[10.8].data - RGB_raster_merc[12.0].data
        BTD2_RGB_raster = RGB_raster_merc[10.8].data - RGB_raster_merc[8.7].data

        img_RGB_raster_path = '%s/%d%02d%02d%02d%02d_%s_RGB.tif' % (path_final_product, yyyy, mm, dd, HH, MM, zone)
        img_RGB_raster = GeoImage((BTD1_RGB_raster,
                                   BTD2_RGB_raster,
                                   RGB_raster_merc[10.8].data),
                                  zone_merc,
                                  time_slot,
                                  mode="RGB")
        img_RGB_raster.enhance(stretch="crude", gamma=1)
        img_RGB_raster.geotiff_save(img_RGB_raster_path,
                                    compression=0,
                                    geotransform=geotrf,
                                    spatialref=srs)
        # **** Fin - CrÃ©ation gÃ©otiff RGB raster ****

        # **** DÃ©but - CrÃ©ation gÃ©otiff SO2 low altitude ****
        data[3.9].data = SO2_LA_semiflag_red
        data[8.7].data = SO2_LA_semiflag_green
        data[10.8].data = SO2_LA_semiflag_blue

        SO2_LA_merc = data.project(zone_merc)

        img_SO2_LA_path = '%s/%d%02d%02d%02d%02d_%s_SO2_LA.tif' % (path_final_product, yyyy, mm, dd, HH, MM, zone)
        img_SO2_LA = GeoImage((SO2_LA_merc[3.9].data,
                               SO2_LA_merc[8.7].data,
                               SO2_LA_merc[10.8].data),
                              zone_merc,
                              time_slot,
                              mode="RGB")
        img_SO2_LA.enhance(stretch="crude", gamma=1)
        img_SO2_LA.geotiff_save(img_SO2_LA_path,
                                compression=0,
                                geotransform=geotrf,
                                spatialref=srs)
        # **** Fin - CrÃ©ation gÃ©otiff SO2 low altitude ****

        # **** DÃ©but - CrÃ©ation gÃ©otiff SO2 high altitude ****
        data[3.9].data = SO2_HA_semiflag_red
        data[8.7].data = SO2_HA_semiflag_green
        data[10.8].data = SO2_HA_semiflag_blue

        SO2_HA_merc = data.project(zone_merc)

        img_SO2_HA_path = '%s/%d%02d%02d%02d%02d_%s_SO2_HA.tif' % (path_final_product, yyyy, mm, dd, HH, MM, zone)
        img_SO2_HA = GeoImage((SO2_HA_merc[3.9].data,
                               SO2_HA_merc[8.7].data,
                               SO2_HA_merc[10.8].data),
                              zone_merc,
                              time_slot,
                              mode="RGB")
        img_SO2_HA.enhance(stretch="crude", gamma=1)
        img_SO2_HA.geotiff_save(img_SO2_HA_path,
                                compression=0,
                                geotransform=geotrf,
                                spatialref=srs)

        # **** Fin - CrÃ©ation gÃ©otiff SO2 low altitude ****

        # **** CrÃ©ation des pyramides ****
        def create_tiles(image_product, product):
            """ creation des pyramides avec gdal2tiles.py
            retourne object Popen
            :rtype : object """
            GDAL_EXEC = '/usr/bin/gdal2tiles.py'
            sys.argv = [GDAL_EXEC, '-r', 'near', '-z1-8', image_product]
            sys.argv.append('{0}/{1}'.format(path_final_product, product))
            p = Popen(sys.argv, close_fds=True)
            return p

        create_tiles(img_ASH2_semiflag_path, 'ASH2')
        create_tiles(img_ASH3_semiflag_path, 'ASH3')
        create_tiles(img_Cloud_raster_path, 'Cloud')
        create_tiles(img_NTI_raster_path, 'NTI_raster')
        create_tiles(img_NTI_flag_path, 'NTI_flag')
        create_tiles(img_RGB_raster_path, 'RGB')
        create_tiles(img_SO2_LA_path, 'SO2_LA')
        last_tiles = create_tiles(img_SO2_HA_path, 'SO2_HA')
        last_tiles.wait()

        # **** Compression des gÃ©otifs dans une archive tar.bz2 ****
        files_to_compress = []
        files_to_compress.append(img_ASH2_semiflag_path)
        files_to_compress.append(img_ASH3_semiflag_path)
        files_to_compress.append(img_Cloud_raster_path)
        files_to_compress.append(img_NTI_raster_path)
        files_to_compress.append(img_NTI_flag_path)
        files_to_compress.append(img_RGB_raster_path)
        files_to_compress.append(img_SO2_LA_path)
        files_to_compress.append(img_SO2_HA_path)

        tarfile_name = '%s/%d%02d%02d%02d%02d_%s_geotifs.tar.bz2' % (path_final_product, yyyy, mm, dd, HH, MM, zone)
        tar = tarfile.open(tarfile_name, 'w:bz2')
        for name in files_to_compress:
            tar.add(name, arcname=name[(len(path_final_product) + 1):])
            if os.path.exists(name):
                try:
                    os.remove(name)
                except OSError, e:
                    print
                    'Error: %s - %s' % (e.filename, e.strerror)
        tar.close()

        # **** DÃ©but - Sauvegarde des rÃ©sultats dans la BDD ****

        # **** RÃ©cupÃ©ration des limites pour lÃ©gender images sur google map
        minASH2 = numpy.ma.min(ASH2_semiflag) if ASH2_semiflag.count() > 0 else 0.0
        maxASH2 = numpy.ma.max(ASH2_semiflag) if ASH2_semiflag.count() > 0 else 0.0
        minASH3 = numpy.ma.min(ASH3_semiflag) if ASH3_semiflag.count() > 0 else 0.0
        maxASH3 = numpy.ma.max(ASH3_semiflag) if ASH3_semiflag.count() > 0 else 0.0
        minRAD = numpy.ma.min(NTI_raster) if NTI_raster.count() > 0 else 0.0
        maxRAD = numpy.ma.max(NTI_raster) if NTI_raster.count() > 0 else 0.0
        minCLOUD = numpy.ma.min(Cloud_raster) if Cloud_raster.count() > 0 else 0.0
        maxCLOUD = numpy.ma.max(Cloud_raster) if Cloud_raster.count() > 0 else 0.0
        minSO2_LA = numpy.ma.min(SO2_LA_semiflag) if SO2_LA_semiflag.count() > 0 else 0.0
        maxSO2_LA = numpy.ma.max(SO2_LA_semiflag) if SO2_LA_semiflag.count() > 0 else 0.0
        minSO2_HA = numpy.ma.min(SO2_HA_semiflag) if SO2_HA_semiflag.count() > 0 else 0.0
        maxSO2_HA = numpy.ma.max(SO2_HA_semiflag) if SO2_HA_semiflag.count() > 0 else 0.0

        SQL_legend = "SELECT UPERT_INTO_hotvolc_legend_label\
                            (TIMESTAMP %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
        DATA = (time_slot,
                zone,
                minASH2,
                maxASH2,
                minASH3,
                maxASH3,
                minRAD,
                maxRAD,
                minCLOUD,
                maxCLOUD,
                cloudCOVER,
                minSO2_LA,
                maxSO2_LA,
                minSO2_HA,
                maxSO2_HA)
        # **** Connection Ã  la BDD et execution des requetes ****
        con = hotvolc_database.DB_connection()
        try:
            with con:
                with con.cursor() as cur:
                    cur.execute(SQL_legend, DATA)

            # **** Mise en forme des enregistrement monitoring atmospherique ****
            lat = numpy.round(lat, 4)
            lon = numpy.round(lon, 4)

            # Parcours de la liste des volcans
            DATA = []
            for volcan in volcans:
                lat_volc = volcan['lat']
                lon_volc = volcan['lon']
                offN = volcan['offsetN_atmos']
                offS = volcan['offsetS_atmos']
                offE = volcan['offsetE_atmos']
                offW = volcan['offsetW_atmos']

                # Calcul de la position du volcan sur l'image
                dist = hotvolc_utils.distOnSphere(lat, lon, lat_volc, lon_volc)
                idx = numpy.where(dist == numpy.min(dist))

                # RÃ©cupÃ©ration de la zone d'interet
                BTD_ROI_ASH2 = ASH2_semiflag[idx[0][0] - offN:idx[0][0] + offS + 1,
                               idx[1][0] - offW:idx[1][0] + offE + 1]
                Tb108_ROI_ASH2 = Tb108_ASH2[idx[0][0] - offN:idx[0][0] + offS + 1,
                                 idx[1][0] - offW:idx[1][0] + offE + 1]
                BTD_ROI_ASH3 = ASH3_semiflag[idx[0][0] - offN:idx[0][0] + offS + 1,
                               idx[1][0] - offW:idx[1][0] + offE + 1]
                Tb108_ROI_ASH3 = Tb108_ASH3[idx[0][0] - offN:idx[0][0] + offS + 1,
                                 idx[1][0] - offW:idx[1][0] + offE + 1]
                SO2_LA_ROI = SO2_LA_semiflag[idx[0][0] - offN:idx[0][0] + offS + 1,
                             idx[1][0] - offW:idx[1][0] + offE + 1]
                SO2_HA_ROI = SO2_HA_semiflag[idx[0][0] - offN:idx[0][0] + offS + 1,
                             idx[1][0] - offW:idx[1][0] + offE + 1]

                nb_ROI_ASH2 = BTD_ROI_ASH2.count()
                nb_ROI_ASH3 = BTD_ROI_ASH3.count()
                nb_ROI_SO2_LA = SO2_LA_ROI.count()
                nb_ROI_SO2_HA = SO2_HA_ROI.count()

                min_BTD_ROI_ASH2 = numpy.ma.min(BTD_ROI_ASH2) if nb_ROI_ASH2 > 0 else 0.0
                min_Tb108_ROI_ASH2 = numpy.ma.min(Tb108_ROI_ASH2) if nb_ROI_ASH2 > 0 else 0.0
                min_BTD_ROI_ASH3 = numpy.ma.min(BTD_ROI_ASH3) if nb_ROI_ASH3 > 0 else 0.0
                min_Tb108_ROI_ASH3 = numpy.ma.min(Tb108_ROI_ASH3) if nb_ROI_ASH3 > 0 else 0.0
                max_SRD_SO2_LA = numpy.ma.max(SO2_LA_ROI) if nb_ROI_SO2_LA > 0 else 0.0
                max_SRD_SO2_HA = numpy.ma.max(SO2_HA_ROI) if nb_ROI_SO2_HA > 0 else 0.0

                DATA.append((time_slot,
                             zone,
                             volcan['volcan'],
                             nb_ROI_ASH2,
                             min_Tb108_ROI_ASH2,
                             min_BTD_ROI_ASH2,
                             nb_ROI_ASH3,
                             min_Tb108_ROI_ASH3,
                             min_BTD_ROI_ASH3,
                             nb_ROI_SO2_LA,
                             max_SRD_SO2_LA,
                             nb_ROI_SO2_HA,
                             max_SRD_SO2_HA))
            SQL_ash_monitoring_volcan = "SELECT UPERT_INTO_hotvolc_atmospheric_monitoring\
                                (TIMESTAMP %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
            with con:
                with con.cursor() as cur:
                    cur.executemany(SQL_ash_monitoring_volcan, DATA)

            # **** RÃ©cupÃ©ration des donnÃ©es thermal monitoring ****
            SQL_thermal_monitoring = "SELECT UPERT_INTO_hotvolc_thermal_monitoring\
                                    ( TIMESTAMP %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
            DATA = []
            for volcan in NTI_monitoring:
                DATA.append((time_slot,
                             zone,
                             volcan['volcan'],
                             volcan['sizeROI'],
                             volcan['nbAnomalies'],
                             volcan['TSR'],
                             volcan['r39mean'],
                             volcan['r39max'],
                             volcan['r39min'],
                             volcan['r12mean'],
                             volcan['r12max'],
                             volcan['r12min']))
            with con:
                with con.cursor() as cur:
                    cur.executemany(SQL_thermal_monitoring, DATA)

            # **** NTI monitoring ****
            SQL_nti_monitoring = "SELECT UPERT_INTO_hotvolc_nti_monitoring\
                                 (TIMESTAMP %s, %s, %s, %s, %s, %s);"
            DATA = []
            for pixel in NTI_monitoring_volcan:
                DATA.append((time_slot,
                             zone,
                             pixel['volcan'],
                             pixel['pixID'],
                             float(pixel['nti']),
                             pixel['anomaly']))
            with con:
                with con.cursor() as cur:
                    cur.executemany(SQL_nti_monitoring, DATA)

            # **** RÃ©cupÃ©ration des donnÃ©es thermal anomalies ****
            SQL_thermal_anomalie = "SELECT UPERT_INTO_hotvolc_thermal_anomalies" \
                                   "( TIMESTAMP %s, %s, %s, %s, %s, %s, %s, %s);"
            DATA = []
            for anomalie in NTI_anomalies:
                DATA.append((time_slot,
                             zone,
                             anomalie['volcan'],
                             anomalie['lat'],
                             anomalie['lon'],
                             anomalie['r39'],
                             anomalie['r12'],
                             anomalie['nti']))
            with con:
                with con.cursor() as cur:
                    cur.executemany(SQL_thermal_anomalie, DATA)

            SQL_Lava_Volume = "SELECT UPERT_INTO_hotvolc_lava_volume\
                             ( TIMESTAMP %s, %s, %s, %s, %s, %s, %s);"
            DATA = []
            for volcan in Lava_Volume:
                DATA.append((time_slot,
                             zone,
                             volcan['volcan'],
                             volcan['volume'],
                             volcan['Tamb'],
                             volcan['Tsurf'],
                             volcan['h']))
            with con:
                with con.cursor() as cur:
                    cur.executemany(SQL_Lava_Volume, DATA)
            del DATA

            SQL_quality_flag = "SELECT UPERT_INTO_hotvolc_quality_flag " \
                               "(TIMESTAMP %s, %s, %s, %s, %s, %s);"
            DATA = []
            for volcan in quality_dic:
                DATA.append((time_slot,
                             zone,
                             volcan['volcan'],
                             volcan['Tb108_mean'],
                             volcan['Cloud_Cover'],
                             volcan['Quality_Flag']))
            with con:
                with con.cursor() as cur:
                    cur.executemany(SQL_quality_flag, DATA)
            del DATA

        except psycopg2.DatabaseError, e:
            if con:
                con.rollback()
                hotvolc_database.DB_close(con)
            print
            'Error %s' % e
            sys.exit(1)
        hotvolc_database.DB_close(con)
