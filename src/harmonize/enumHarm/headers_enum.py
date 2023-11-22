from enum import Enum


class HeadersEnum(Enum):
    # LIBRARY DATABASE TABLES HEADERS
    DWELLING_COLUMNS = ['type', 'survey_id', 'ethnic_territoriality', 'ethnic_territoriality_type',
                        'ethnic_territoriality_code', 'protected_area', 'protected_area_code', 'use', 'dwelling_type',
                        'occupancy_status', 'household_number', 'wall_material', 'floor_material',
                        'electricity_service', 'social_stratum', 'aqueduct_service', 'sewerage_service', 'gas_service',
                        'rubbish_service', 'rubbish_service_per_week', 'internet_service', 'sanitary_service',
                        'institution_type', 'has_household', 'total_residents']
    HOUSEHOLD_COLUMNS = ['household_id', 'survey_id', 'hosehold_in_dwelling', 'room_number', 'bedroom_number',
                         'kitchen_type', 'water_source', 'desceased_number']
    GEOREF_COLUMNS = ['department', 'municipality', 'class', 'locality', 'rural_sector', 'rural_section',
                      'populated_center', 'urban_sector', 'urban_section', 'block', 'building_number', 'survey_id',
                      'housing_number']
    PERSON_COLUMNS = ['person_id', 'survey_id', 'hosehold_in_dwelling', 'person_number', 'sex', 'age_group',
                      'head_relationship', 'ethnic_group', 'indigenous_group', 'occupancy_status', 'clan', 'vitsa',
                      'kumpania', 'speaks_native_language', 'understands_native_language', 'other_native_languages',
                      'number_native_languages', 'birthplace', 'residence_five_years', 'residence_one_year',
                      'health_problem', 'main_treatment', 'cared_health_problem', 'heath_service_quality',
                      'daily_life_difficulty', 'read_write', 'school_attendance', 'education_level',
                      'past_week_activities', 'marital_status', 'born_children', 'total_children', 'male_children',
                      'female_children', 'surviving_children', 'surviving_total', 'surviving_male', 'surviving_female',
                      'outside_children', 'outside_total', 'outside_males', 'outside_females', 'last_child_born',
                      'last_child_born_month', 'last_child_born_year']
    DECEASED_COLUMNS = ['deceased_id', 'survey_id', 'deceased_number', 'household_in_dwelling', 'sex', 'age_death',
                        'death_certificate']

    # COLOMBIA CSV HEADERS
    COL_DWELLING_COLUMNS = ['TIPO_REG', 'COD_ENCUESTAS', 'UVA_ESTATER', 'UVA1_TIPOTER', 'UVA2_CODTER',
                            'UVA_ESTA_AREAPROT', 'UVA1_COD_AREAPROT', 'UVA_USO_UNIDAD', 'V_TIPO_VIV', 'V_CON_OCUP',
                            'V_TOT_HOG', 'V_MAT_PARED', 'V_MAT_PISO', 'VA_EE', 'VA1_ESTRATO', 'VB_ACU', 'VC_ALC',
                            'VD_GAS', 'VE_RECBAS', 'VE1_QSEM', 'VF_INTERNET', 'V_TIPO_SERSA', 'L_TIPO_INST',
                            'L_EXISTEHOG', 'L_TOT_PERL']
    COL_HOUSEHOLD_COLUMNS = ['COD_ENCUESTAS', 'H_NROHOG', 'H_NRO_CUARTOS', 'H_NRO_DORMIT', 'H_DONDE_PREPALIM',
                             'H_AGUA_COCIN', 'HA_NRO_FALL', 'HA_TOT_PER']
    COL_GEOREF_COLUMNS = ['U_DPTO', 'U_MPIO', 'UA_CLASE', 'UA1_LOCALIDAD', 'U_SECT_RUR', 'U_SECC_RUR', 'UA2_CPOB',
                          'U_SECT_URB', 'U_SECC_URB', 'U_MZA', 'U_EDIFICA', 'COD_ENCUESTAS', 'U_VIVIENDA']
    COL_PERSON_COLUMNS = ['COD_ENCUESTAS', 'P_NROHOG', 'P_NRO_PER', 'P_SEXO', 'P_EDADR', 'P_PARENTESCOR',
                          'PA1_GRP_ETNIC', 'PA11_COD_ETNIA', 'PA12_CLAN', 'PA21_COD_VITSA', 'PA22_COD_KUMPA',
                          'PA_HABLA_LENG', 'PA1_ENTIENDE', 'PB_OTRAS_LENG', 'PB1_QOTRAS_LENG', 'PA_LUG_NAC',
                          'PA_VIVIA_5ANOS', 'PA_VIVIA_1ANO', 'P_ENFERMO', 'P_QUEHIZO_PPAL', 'PA_LO_ATENDIERON',
                          'PA1_CALIDAD_SERV', 'CONDICION_FISICA', 'P_ALFABETA', 'PA_ASISTENCIA', 'P_NIVEL_ANOSR',
                          'P_TRABAJO', 'P_EST_CIVIL', 'PA_HNV', 'PA1_THNV', 'PA2_HNVH', 'PA3_HNVM', 'PA_HNVS',
                          'PA1_THSV', 'PA2_HSVH', 'PA3_HSVM', 'PA_HFC', 'PA1_THFC', 'PA2_HFCH', 'PA3_HFCM', 'PA_UHNV',
                          'PA1_MES_UHNV', 'PA2_ANO_UHNV']
    COL_DECEASED_COLUMNS = ['COD_ENCUESTAS', 'F_NROHOG', 'FA1_NRO_FALL', 'FA2_SEXO_FALL', 'FA3_EDAD_FALL',
                            'FA4_CERT_DEFUN']
