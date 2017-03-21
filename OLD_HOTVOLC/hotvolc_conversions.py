#!/usr/bin/python
# -*- coding: utf8 -*-
import os

import numpy


def CN2RadEff(CN, can, chemin, yyyy, mm, dd, HH, MM):
    """Fonction de conversion des donnÃ©es MSG du compte numÃ©rique Ã  la radiance effective
    unitÃ© de la radiance effective : mW.m-2.sr-1.(cm-1)-1"""

    file_CAL = "%s%d%d%d/MSG_0/CAL%d%d%d%d%d" % (chemin, yyyy, mm, dd, yyyy, mm, dd, HH, MM)
    cal = []

    if os.path.isfile(file_CAL):
        with open(file_CAL) as fic:
            for line in fic:
                temp = [(count, float(value)) for count, value in enumerate(line.split(' '), 1)]
                dict_vars = {}
                for count, value in temp:
                    dict_vars['Elt' + str(count)] = value
                cal.append(dict_vars)
    else:
        # Cas oÃ¹ le fichier CAL n'existe pas... On force les valeurs derniÃ¨res valeurs connues le 2015/07/31
        CAL_OFF = [-1.065268, -1.421905, -1.202993, -0.186592, -0.424224, -1.969720, -6.463960, -5.302006,
                   -10.456820, -11.337869, -8.037951, -1.907242]
        CAL_SLO = [0.020888, 0.027880, 0.023588, 0.003659, 0.008318, 0.038622, 0.126744, 0.103961,
                   0.205036, 0.222311, 0.157607, 0.037397]

        i = 0
        while i < len(CAL_OFF):
            buff = {'Elt1': CAL_OFF[i], 'Elt2': CAL_SLO[i]}
            cal.append(buff)
            i += 1

    radE = cal[can - 1]['Elt1'] + cal[can - 1]['Elt2'] * CN

    return radE


def RadEff2Tb(radE, can):
    """Fonction de conversion des donnÃ©es de la radiance effective vers la tempÃ©rature de brillance"""

    wave_length = [0.6, 0.8, 1.6, 3.9, 6.2, 7.3, 8.7, 9.7, 10.8, 12.0, 13.4]
    v = 1e4 / wave_length[can - 1]

    c1 = 1.19104e-5
    c2 = 1.43877

    Tb = numpy.real((c2 * v) / numpy.ma.log(1 + (v * v * v) * (c1 / radE)))

    return Tb


def Tb2RadSpe(Tb, can):
    """Fonction de conversion des donnÃ©es en TempÃ©rature de Brillance vers des radiances sepctrales"""

    wave_length = [0.6, 0.8, 1.6, 3.9, 6.2, 7.3, 8.7, 9.7, 10.8, 12.0, 13.4]

    # constantes
    H = 6.626068e-34
    C = 2.997925e8
    K = 1.38066e-23

    l = wave_length[can - 1] * 1e-6
    Beta = (H * C) / (K * l * Tb)
    Alpha = (2 * H * C * C) / (l * l * l * l * l)
    radS = (Alpha / (numpy.exp(Beta) - 1)) * 1e-6

    return radS
