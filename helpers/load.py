import os 
import re
import pandas as pd

def initialize_database(dbManager):
    try:
        query = """
        CREATE TABLE IF NOT EXISTS servings (
            day                     TEXT NOT NULL,
            \"group\"                   TEXT,
            food_name               TEXT NOT NULL,
            amount                  TEXT NOT NULL,
            energy_kcal             FLOAT,
            alcohol_g               FLOAT,
            caffeine_mg             FLOAT,
            water_g                 FLOAT,
            b1_thiamine_mg          FLOAT,
            b2_riboflavin_mg        FLOAT,
            b3_niacin_mg            FLOAT,
            b5_pantothenic_acid_mg  FLOAT,
            b6_pyridoxine_mg        FLOAT,
            b12_cobalamin_g         FLOAT,
            folate_g                FLOAT,
            vitamin_a_g             FLOAT,
            vitamin_c_mg            FLOAT,
            vitamin_d_iu            FLOAT,
            vitamin_e_mg            FLOAT,
            vitamin_k_g             FLOAT,
            calcium_mg              FLOAT,
            copper_mg               FLOAT,
            iron_mg                 FLOAT,
            magnesium_mg            FLOAT,
            manganese_mg            FLOAT,
            phosphorus_mg           FLOAT,
            potassium_mg            FLOAT,
            selenium_g              FLOAT,
            sodium_mg               FLOAT,
            zinc_mg                 FLOAT,
            carbs_g                 FLOAT,
            fiber_g                 FLOAT,
            starch_g                FLOAT,
            sugars_g                FLOAT,
            added_sugars_g          FLOAT,
            net_carbs_g             FLOAT,
            fat_g                   FLOAT,
            cholesterol_mg          FLOAT,
            monounsaturated_g       FLOAT,
            polyunsaturated_g       FLOAT,
            saturated_g             FLOAT,
            transfats_g             FLOAT,
            omega3_g                FLOAT,
            omega6_g                FLOAT,
            cystine_g               FLOAT,
            histidine_g             FLOAT,
            isoleucine_g            FLOAT,
            leucine_g               FLOAT,
            lysine_g                FLOAT,
            methionine_g            FLOAT,
            phenylalanine_g         FLOAT,
            protein_g               FLOAT,
            threonine_g             FLOAT,
            tryptophan_g            FLOAT,
            tyrosine_g              FLOAT,
            valine_g                FLOAT,
            category                TEXT
        )
        """
        dbManager.execute_query(query)
    except Exception as err:
        print("Error occurred: " + err)

def load_csv_to_postgres(dbManager):
    return None