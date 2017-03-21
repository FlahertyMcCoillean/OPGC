#!/usr/bin/python
# -*-coding:Utf-8 -*

import hotvolc_database
import hotvolc_database_function_creator
import hotvolc_database_trigger
import hotvolc_init_tables

# hotvolc_database_copy.database_save()
hotvolc_database_trigger.drop_trigger_fille_petitefille()
hotvolc_database_trigger.drop_trigger_mere_fille()
hotvolc_database_function_creator.hotvolc_supp_function()
hotvolc_database.delete_tables()
hotvolc_database.create_tables()

hotvolc_init_tables.init_table_path()
hotvolc_init_tables.init_table_zones()
# hotvolc_init_tables.init_table_volcans()
hotvolc_init_tables.init_table_volcans_monitoring()
hotvolc_database_trigger.create_trigger_mere_fille()
hotvolc_database_trigger.create_trigger_fille_petitefille()
hotvolc_database_function_creator.hotvolc_create_function()
# hotvolc_database_copy.database_restore()
