SELECT T.*,
       (SELECT ld.ID
          FROM D_LPU l
               JOIN D_LPUDICT ld ON ld.ID = l.LPUDICT
         WHERE l.ID =T.LPU) LPUDICT_ID,
       (SELECT ld.LPU_NAME
          FROM D_LPU l
               JOIN D_LPUDICT ld ON ld.ID = l.LPUDICT
         WHERE l.ID =T.LPU) LPU_NAME,
       (SELECT ld.LPU_CODE
          FROM D_LPU l
               JOIN D_LPUDICT ld ON ld.ID = l.LPUDICT
         WHERE l.ID =T.LPU) LPU_CODE
  FROM (SELECT ffd.ID,
               ffd.MO_OID,
               (SELECT l.ID
                 FROM D_FN_REGISTER_MO2 frm
                      JOIN d_fed_nsi_links fnl ON fnl.unit_id = frm.ID
                      JOIN d_fed_nsi_link_sp fnls ON fnls.PID = fnl.ID AND fnls.UNITCODE = 'LPUDICT'
                      JOIN D_LPUDICT ld ON ld.id = fnls.unit_id
                      JOIN D_LPU l ON l.LPUDICT = ld.ID
                WHERE frm.vers = D_PKG_FED_NSI.GET_LAST_VERS(psUNITCODE =>'FN_REGISTER_MO2',
                                                             pnRAISE    => 0)
                      AND frm.oid = ffd.MO_OID
                      AND ROWNUM<=1) LPU,
               ffd.DEPART_OID,
               ffd.DEPART_NAME,
               ffd.vers,
               G.ID GEO_ID,
               G.PID GEO_PARENT,
               G.KLADR_CODE,
               G.GEOFULL,
               G.fias_code,
               G.GEOLOCTYPE,
               gt.code geotype_code,
               gt.name geotype_name,
               MAX(ffd.DEPART_OID) OVER (PARTITION BY G.ID) max_div_oid
         FROM D_FN_FRMO_DIV ffd
              JOIN D_GEOGRAFY g ON ffd.BUILDING_ADDRESS_AOID_AREA = g.FIAS_CODE
              LEFT JOIN D_GEOGRAFYTYPES gt ON gt.ID= G.GEOLOCTYPE
          WHERE (lower(ffd.DEPART_NAME) LIKE '%фап%' OR lower(ffd.DEPART_NAME) LIKE '%фельдшерско%')
                AND ffd.vers = D_PKG_FED_NSI.GET_LAST_VERS(psUNITCODE =>'FN_FRMO_DIV',
                                                           pnRAISE    => 0)
                AND ffd.MO_OID LIKE '1.2.643.5.1.13.13.12.2.54.%'
                AND ffd.DEPART_LIQUIDATION_DATE IS NULL
                AND g.ID NOT IN (1802048, --Новосибирск
                      1802050, --Бердск
                      1802053, --Искитим
                      1802051, --г. Обь
                      1802086, --Кольцово
                      1802142, --Баган
                      1802052, --Барабинск
                      1802238, --Болотное
                      1802300, --Венгерово
                      1802351, --Довольное
                      1802379, --Здвинск
                      1802493, --Карасук
                      1802550, --Каргат
                      1802592, --Колывань
                      1802651, --Коченево
                      1802718, --Кочки
                      1802759, --Краснозерское
                      1802054,--Куйбышев
                      19585565179, --Куйбышев
                      1802893, --Купино
                      1802955, --Кыштовка
                      1803013, --Маслянино
                      1803046, --Мошково
                      1803097, --Ордынское
                      1803138, --Северное
                      1803171, --Сузун
                      1802055, --Татарск
                      19585565180, --Татарск
                      1803289, --Тогучин
                      1803397, --Убинское
                      1803449, --Усть-Тарка
                      1803489,--Чаны
                      1803550, --Черепаново
                      1803599, --Чистоозерное
                      1803650 --Чулым
                      )
         ) t
 WHERE DEPART_OID = max_div_oid