#!/usr/bin/python
# -*-coding:Utf-8 -*

import datetime

import numpy
from Tkinter import *
from hotvolc_utils import aeCalcMerc, aeCalcGeos
from mpop.satellites import GeostationaryFactory


def actualise():
    # ATTENTION - PARAMETRES A CONFIGURER
    # Pour fonctionner ce programme a besoin de lancer une extraction Ã  l'aide de MIPP
    # Veillez Ã  avoir un set de donnÃ©es disponible et configurez ces variables en consÃ©quence
    nom_satellite = 'meteosat'
    num_satellite = '09'
    nom_capteur = 'seviri'
    annee = 2014
    mois = 12
    jour = 14
    heure = 14
    minute = 30
    facteur_taille = 1.5
    # ATTENTION

    # On efface le contenu des deux text_box
    text_geos.delete("1.0", END)
    text_merc.delete("1.0", END)

    # Conversion des latitude/longitude d'une chaine de caractÃ¨res vers un flottant
    lonmin = float(text_lonmin.get())
    lonmax = float(text_lonmax.get())
    latmin = float(text_latmin.get())
    latmax = float(text_latmax.get())

    # Calcul de l'area extent pour la projection satellite
    ae_geos = aeCalcGeos(lonmin, lonmax, latmin, latmax)
    # Mise en forme en chaine de caractÃ¨res
    x0_geos = "%.10f" % ae_geos[0]
    y0_geos = "%.10f" % ae_geos[1]
    x1_geos = "%.10f" % ae_geos[2]
    y1_geos = "%.10f" % ae_geos[3]

    # Extractuion des donnÃ©es pour dÃ©terminer la taille en pixel des images
    time_slot = datetime.datetime(annee, mois, jour, heure, minute)
    data = GeostationaryFactory.create_scene(nom_satellite, num_satellite, nom_capteur, time_slot)
    data.load([3.9], area_extent=ae_geos, calibrate=1)
    # RÃ©cupÃ©ration de la taille en pixels de l'extraction satellite
    size_data = numpy.shape(data[3.9].data)

    # Calcul de l'area extent pour la projection mercator
    ae_merc = aeCalcMerc(lonmin, lonmax, latmin, latmax)
    # Mise au format chaine de caractÃ¨res
    x0_merc = "%.10f" % ae_merc[0]
    y0_merc = "%.10f" % ae_merc[1]
    x1_merc = "%.10f" % ae_merc[2]
    y1_merc = "%.10f" % ae_merc[3]

    # Calcul de la taille en pixel des images reprojetÃ©es en mercator
    # ATTENTION: la facteur taille doit etre au moins de 1.5 pour Ã©viter un sous-rÃ©Ã©chantillonage
    facteur = (ae_merc[0] - ae_merc[2]) / (ae_merc[1] - ae_merc[3])
    xsize = round(facteur_taille * size_data[1])
    ysize = round(xsize / facteur)
    # Mise au format chaine de caractÃ¨re
    XSIZE = "%.0f" % xsize
    YSIZE = "%.0f" % ysize

    # Ecriture dans la texte box : text_geos
    text_geos.insert(END, "REGION: " + text_nom.get() + "_sp {\n")
    text_geos.insert(END, "        NAME:          " + text_desc.get() + " - En projection satellite\n")
    text_geos.insert(END, "        PCS_ID:        geos0\n")
    text_geos.insert(END, "        PCS_DEF:       proj=geos, lon_0=0.0, a=6378169.00, b=6356583.80, h=35785831.0\n")
    text_geos.insert(END, "        XSIZE:         " + str(size_data[1]) + "\n")
    text_geos.insert(END, "        YSIZE:         " + str(size_data[0]) + "\n")
    text_geos.insert(END,
                     "        AREA_EXTENT:   (" + x0_geos + ", " + y0_geos + ", " + x1_geos + ", " + y1_geos + ")\n};\n")

    # Ecriture dans la texte box : text_merc
    text_merc.insert(END, "REGION: " + text_nom.get() + "_mp {\n")
    text_merc.insert(END, "        NAME:          " + text_desc.get() + " - En projection mercator\n")
    text_merc.insert(END, "        PCS_ID:        merc\n")
    text_merc.insert(END, "        PCS_DEF:       proj=merc, ellps=WGS84, lon_0=0\n")
    text_merc.insert(END, "        XSIZE:         " + XSIZE + "\n")
    text_merc.insert(END, "        YSIZE:         " + YSIZE + "\n")
    text_merc.insert(END,
                     "        AREA_EXTENT:   (" + x0_merc + ", " + y0_merc + ", " + x1_merc + ", " + y1_merc + ")\n};\n")


# Creation de la fenetre du programme
fenetre = Tk()
fenetre.title('Calcul des zones d\'extraction pour mpop')

# Configuration du premier champ qui permet de rÃ©cupÃ©rer le nom de la zone d'extraction
Label(fenetre, text="Nom de la zone :").grid(row=1, column=1, columnspan=4)
text_nom = StringVar()
nom_entry = Entry(fenetre, width=15, textvariable=text_nom).grid(row=2, column=1, columnspan=4)
Label(fenetre, text="").grid(row=3)

# Configuration du deuxiÃ¨me champ qui permet de rÃ©cupÃ©rer la dÃ©scription de la zone d'extraction
Label(fenetre, text="Description :").grid(row=4, column=1, columnspan=4)
text_desc = StringVar()
desc_entry = Entry(fenetre, width=30, textvariable=text_desc).grid(row=5, column=1, columnspan=4)

# Ajout d'une ligne vide (critÃ¨re esthÃ©tique)
Label(fenetre, text="").grid(row=6)

# Configuration des champs qui permettent de rÃ©cupÃ©rer les longitudes limites
Label(fenetre, text="Lon-min :").grid(row=7, column=1)
text_lonmin = StringVar()
lonmin_entry = Entry(fenetre, width=8, textvariable=text_lonmin).grid(row=7, column=2)
Label(fenetre, text="Lon-max :").grid(row=7, column=3)
text_lonmax = StringVar()
lonmax_entry = Entry(fenetre, width=8, textvariable=text_lonmax).grid(row=7, column=4)

# Configuration des champs qui permettent de rÃ©cupÃ©rer les latitudes limites
Label(fenetre, text="Lat-min :").grid(row=9, column=1)
text_latmin = StringVar()
latmin_entry = Entry(fenetre, width=8, textvariable=text_latmin).grid(row=9, column=2)
Label(fenetre, text="Lat-max :").grid(row=9, column=3)
text_latmax = StringVar()
latmax_entry = Entry(fenetre, width=8, textvariable=text_latmax).grid(row=9, column=4)

# Ajout d'une ligne vide (critÃ¨re esthÃ©tique)
Label(fenetre, text="").grid(row=10)

# Configuration du bouton qui permet de lancer les calculs
Button(fenetre, text="GÃ©nÃ©rer les paramÃ¨tres d'extraction", command=actualise).grid(row=11, column=1, columnspan=4)
Label(fenetre, text="").grid(row=1, column=5)

# Configuration de la zone de texte oÃ¹ va etre affichÃ© le rÃ©sultat pour la projection satellite
Label(fenetre, text="En projection satellite :").grid(row=1, column=6)
text_geos = Text(fenetre, bd=1, width=105, height=8)
text_geos.grid(row=2, rowspan=9, column=6)

# Configuration de la zone de texte oÃ¹ va etre affichÃ© le rÃ©sultat pour la projection mercator
Label(fenetre, text="En projection mercator :").grid(row=10, column=6)
text_merc = Text(fenetre, bd=1, width=105, height=8)
text_merc.grid(row=11, rowspan=9, column=6)

fenetre.mainloop()
