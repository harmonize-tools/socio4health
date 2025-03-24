from enum import Enum


class CountryEnum(Enum):
    COLOMBIA = 'COL'
    PERU = 'PER'
    BRAZIL = 'BRA'
    DOMINICAN_REPUBLIC = 'DOM'

class DataSourceTypeEnum(Enum):
    CENSUS = 'CENSUS'
    SURVEY = 'SURVEY'

class BraColnamesEnum(Enum):
    PNADC = [
        "Ano", "Trimestre", "UF", "Capital", "RM_RIDE", "UPA", "Estrato", "V1008", "V1014", "V1016",
        "V1022", "V1023", "V1027", "V1028", "V1029", "V1033", "posest", "posest_sxi", "V2001", "V2003",
        "V2005", "V2007", "V2008", "V20081", "V20082", "V2009", "V2010", "V3001", "V3002", "V3002A",
        "V3003", "V3003A", "V3004", "V3005", "V3005A", "V3006", "V3006A", "V3007", "V3008", "V3009",
        "V3009A", "V3010", "V3011", "V3011A", "V3012", "V3013", "V3013A", "V3013B", "V3014", "V4001",
        "V4002", "V4003", "V4004", "V4005", "V4006", "V4006A", "V4007", "V4008", "V40081", "V40082",
        "V40083", "V4009", "V4010", "V4012", "V40121", "V4013", "V40132", "V40132A", "V4014", "V4015",
        "V40151", "V401511", "V401512", "V4016", "V40161", "V40162", "V40163", "V4017", "V40171",
        "V401711", "V4018", "V40181", "V40182", "V40183", "V4019", "V4020", "V4021", "V4022", "V4024",
        "V4025", "V4026", "V4027", "V4028", "V4029", "V4032", "V4033", "V40331", "V403311", "V403312",
        "V40332", "V403321", "V403322", "V40333", "V403331", "V4034", "V40341", "V403411", "V403412",
        "V40342", "V403421", "V403422", "V4039", "V4039C", "V4040", "V40401", "V40402", "V40403", "V4041",
        "V4043", "V40431", "V4044", "V4045", "V4046", "V4047", "V4048", "V4049", "V4050", "V40501",
        "V405011", "V405012", "V40502", "V405021", "V405022", "V40503", "V405031", "V4051", "V40511",
        "V405111", "V405112", "V40512", "V405121", "V405122", "V4056", "V4056C", "V4057", "V4058",
        "V40581", "V405811", "V405812", "V40582", "V405821", "V405822", "V40583", "V405831", "V40584",
        "V4059", "V40591", "V405911", "V405912", "V40592", "V405921", "V405922", "V4062", "V4062C",
        "V4063", "V4063A", "V4064", "V4064A", "V4071", "V4072", "V4072A", "V4073", "V4074", "V4074A",
        "V4075A", "V4075A1", "V4076", "V40761", "V40762", "V40763", "V4077", "V4078", "V4078A", "V4082",
        "VD2002", "VD2003", "VD2004", "VD2006", "VD3004", "VD3005", "VD3006", "VD4001", "VD4002",
        "VD4003", "VD4004", "VD4004A", "VD4005", "VD4007", "VD4008", "VD4009", "VD4010", "VD4011",
        "VD4012", "VD4013", "VD4014", "VD4015", "VD4016", "VD4017", "VD4018", "VD4019", "VD4020",
        "VD4023", "VD4030", "VD4031", "VD4032", "VD4033", "VD4034", "VD4035", "VD4036", "VD4037",
        "V1028001", "V1028002", "V1028003", "V1028004", "V1028005", "V1028006", "V1028007", "V1028008",
        "V1028009", "V1028010", "V1028011", "V1028012", "V1028013", "V1028014", "V1028015", "V1028016",
        "V1028017", "V1028018", "V1028019", "V1028020", "V1028021", "V1028022", "V1028023", "V1028024",
        "V1028025", "V1028026", "V1028027", "V1028028", "V1028029", "V1028030", "V1028031", "V1028032",
        "V1028033", "V1028034", "V1028035", "V1028036", "V1028037", "V1028038", "V1028039", "V1028040",
        "V1028041", "V1028042", "V1028043", "V1028044", "V1028045", "V1028046", "V1028047", "V1028048",
        "V1028049", "V1028050", "V1028051", "V1028052", "V1028053", "V1028054", "V1028055", "V1028056",
        "V1028057", "V1028058", "V1028059", "V1028060", "V1028061", "V1028062", "V1028063", "V1028064",
        "V1028065", "V1028066", "V1028067", "V1028068", "V1028069", "V1028070", "V1028071", "V1028072",
        "V1028073", "V1028074", "V1028075", "V1028076", "V1028077", "V1028078", "V1028079", "V1028080",
        "V1028081", "V1028082", "V1028083", "V1028084", "V1028085", "V1028086", "V1028087", "V1028088",
        "V1028089", "V1028090", "V1028091", "V1028092", "V1028093", "V1028094", "V1028095", "V1028096",
        "V1028097", "V1028098", "V1028099", "V1028100", "V1028101", "V1028102", "V1028103", "V1028104",
        "V1028105", "V1028106", "V1028107", "V1028108", "V1028109", "V1028110", "V1028111", "V1028112",
        "V1028113", "V1028114", "V1028115", "V1028116", "V1028117", "V1028118", "V1028119", "V1028120",
        "V1028121", "V1028122", "V1028123", "V1028124", "V1028125", "V1028126", "V1028127", "V1028128",
        "V1028129", "V1028130", "V1028131", "V1028132", "V1028133", "V1028134", "V1028135", "V1028136",
        "V1028137", "V1028138", "V1028139", "V1028140", "V1028141", "V1028142", "V1028143", "V1028144",
        "V1028145", "V1028146", "V1028147", "V1028148", "V1028149", "V1028150", "V1028151", "V1028152",
        "V1028153", "V1028154", "V1028155", "V1028156", "V1028157", "V1028158", "V1028159", "V1028160",
        "V1028161", "V1028162", "V1028163", "V1028164", "V1028165", "V1028166", "V1028167", "V1028168",
        "V1028169", "V1028170", "V1028171", "V1028172", "V1028173", "V1028174", "V1028175", "V1028176",
        "V1028177", "V1028178", "V1028179", "V1028180", "V1028181", "V1028182", "V1028183", "V1028184",
        "V1028185", "V1028186", "V1028187", "V1028188", "V1028189", "V1028190", "V1028191", "V1028192",
        "V1028193", "V1028194", "V1028195", "V1028196", "V1028197", "V1028198", "V1028199", "V1028200"
    ]


class BraColspecsEnum(Enum):
    PNADC = [(0, 4), (4, 5), (5, 7), (7, 9), (9, 11), (11, 20), (20, 27), (27, 29), (29, 31), (31, 32), (32, 33), (33, 34),
         (34, 49), (49, 64), (64, 73), (73, 82), (82, 85), (85, 88), (88, 90), (90, 92), (92, 94), (94, 95), (95, 97),
         (97, 99), (99, 103), (103, 106), (106, 107), (107, 108), (108, 109), (109, 110), (110, 112), (112, 114),
         (114, 115), (115, 116), (116, 117), (117, 119), (119, 120), (120, 121), (121, 122), (122, 124), (124, 126),
         (126, 127), (127, 128), (128, 129), (129, 130), (130, 132), (132, 133), (133, 134), (134, 135), (135, 136),
         (136, 137), (137, 138), (138, 139), (139, 140), (140, 141), (141, 142), (142, 143), (143, 144), (144, 146),
         (146, 148), (148, 150), (150, 151), (151, 155), (155, 156), (156, 157), (157, 162), (162, 163), (163, 164),
         (164, 165), (165, 166), (166, 167), (167, 168), (168, 170), (170, 171), (171, 172), (172, 174), (174, 176),
         (176, 177), (177, 178), (178, 179), (179, 180), (180, 181), (181, 183), (183, 185), (185, 186), (186, 187),
         (187, 188), (188, 189), (189, 190), (190, 191), (191, 192), (192, 193), (193, 194), (194, 195), (195, 196),
         (196, 197), (197, 198), (198, 199), (199, 207), (207, 208), (208, 209), (209, 217), (217, 218), (218, 219),
         (219, 220), (220, 221), (221, 222), (222, 230), (230, 231), (231, 232), (232, 240), (240, 243), (243, 246),
         (246, 247), (247, 249), (249, 251), (251, 253), (253, 257), (257, 258), (258, 259), (259, 264), (264, 265),
         (265, 266), (266, 267), (267, 268), (268, 269), (269, 270), (270, 271), (271, 272), (272, 280), (280, 281),
         (281, 282), (282, 290), (290, 291), (291, 292), (292, 293), (293, 294), (294, 295), (295, 303), (303, 304),
         (304, 305), (305, 313), (313, 316), (316, 319), (319, 320), (320, 321), (321, 322), (322, 323), (323, 331),
         (331, 332), (332, 333), (333, 341), (341, 342), (342, 343), (343, 344), (344, 345), (345, 346), (346, 347),
         (347, 355), (355, 356), (356, 357), (357, 365), (365, 368), (368, 371), (371, 372), (372, 373), (373, 374),
         (374, 375), (375, 376), (376, 378), (378, 379), (379, 380), (380, 381), (381, 383), (383, 384), (384, 386),
         (386, 387), (387, 389), (389, 391), (391, 393), (393, 394), (394, 395), (395, 396), (396, 397), (397, 399),
         (399, 401), (401, 402), (402, 404), (404, 405), (405, 407), (407, 408), (408, 409), (409, 410), (410, 411),
         (411, 412), (412, 413), (413, 414), (414, 415), (415, 416), (416, 418), (418, 420), (420, 422), (422, 423),
         (423, 424), (424, 425), (425, 426), (426, 434), (434, 442), (442, 443), (443, 451), (451, 459), (459, 460),
         (460, 461), (461, 464), (464, 467), (467, 470), (470, 473), (473, 476), (476, 477), (477, 478), (478, 493),
         (493, 508), (508, 523), (523, 538), (538, 553), (553, 568), (568, 583), (583, 598), (598, 613), (613, 628),
         (628, 643), (643, 658), (658, 673), (673, 688), (688, 703), (703, 718), (718, 733), (733, 748), (748, 763),
         (763, 778), (778, 793), (793, 808), (808, 823), (823, 838), (838, 853), (853, 868), (868, 883), (883, 898),
         (898, 913), (913, 928), (928, 943), (943, 958), (958, 973), (973, 988), (988, 1003), (1003, 1018),
         (1018, 1033), (1033, 1048), (1048, 1063), (1063, 1078), (1078, 1093), (1093, 1108), (1108, 1123), (1123, 1138),
         (1138, 1153), (1153, 1168), (1168, 1183), (1183, 1198), (1198, 1213), (1213, 1228), (1228, 1243), (1243, 1258),
         (1258, 1273), (1273, 1288), (1288, 1303), (1303, 1318), (1318, 1333), (1333, 1348), (1348, 1363), (1363, 1378),
         (1378, 1393), (1393, 1408), (1408, 1423), (1423, 1438), (1438, 1453), (1453, 1468), (1468, 1483), (1483, 1498),
         (1498, 1513), (1513, 1528), (1528, 1543), (1543, 1558), (1558, 1573), (1573, 1588), (1588, 1603), (1603, 1618),
         (1618, 1633), (1633, 1648), (1648, 1663), (1663, 1678), (1678, 1693), (1693, 1708), (1708, 1723), (1723, 1738),
         (1738, 1753), (1753, 1768), (1768, 1783), (1783, 1798), (1798, 1813), (1813, 1828), (1828, 1843), (1843, 1858),
         (1858, 1873), (1873, 1888), (1888, 1903), (1903, 1918), (1918, 1933), (1933, 1948), (1948, 1963), (1963, 1978),
         (1978, 1993), (1993, 2008), (2008, 2023), (2023, 2038), (2038, 2053), (2053, 2068), (2068, 2083), (2083, 2098),
         (2098, 2113), (2113, 2128), (2128, 2143), (2143, 2158), (2158, 2173), (2173, 2188), (2188, 2203), (2203, 2218),
         (2218, 2233), (2233, 2248), (2248, 2263), (2263, 2278), (2278, 2293), (2293, 2308), (2308, 2323), (2323, 2338),
         (2338, 2353), (2353, 2368), (2368, 2383), (2383, 2398), (2398, 2413), (2413, 2428), (2428, 2443), (2443, 2458),
         (2458, 2473), (2473, 2488), (2488, 2503), (2503, 2518), (2518, 2533), (2533, 2548), (2548, 2563), (2563, 2578),
         (2578, 2593), (2593, 2608), (2608, 2623), (2623, 2638), (2638, 2653), (2653, 2668), (2668, 2683), (2683, 2698),
         (2698, 2713), (2713, 2728), (2728, 2743), (2743, 2758), (2758, 2773), (2773, 2788), (2788, 2803), (2803, 2818),
         (2818, 2833), (2833, 2848), (2848, 2863), (2863, 2878), (2878, 2893), (2893, 2908), (2908, 2923), (2923, 2938),
         (2938, 2953), (2953, 2968), (2968, 2983), (2983, 2998), (2998, 3013), (3013, 3028), (3028, 3043), (3043, 3058),
         (3058, 3073), (3073, 3088), (3088, 3103), (3103, 3118), (3118, 3133), (3133, 3148), (3148, 3163), (3163, 3178),
         (3178, 3193), (3193, 3208), (3208, 3223), (3223, 3238), (3238, 3253), (3253, 3268), (3268, 3283), (3283, 3298),
         (3298, 3313), (3313, 3328), (3328, 3343), (3343, 3358), (3358, 3373), (3373, 3388), (3388, 3403), (3403, 3418),
         (3418, 3433), (3433, 3448), (3448, 3463), (3463, 3478)]