SELECT /*+ MATERIALIZE */
       T.*
  FROM (SELECT T.*,
               MAX(T.ID)  OVER(PARTITION BY T.PID, T.RP_TYPE) MAX_AR_BY_TYPE
          FROM (SELECT ar.ID,
                       ar.PID,
                       ar.LPU_REG,
                       ld.LPU_CODE REF_ID_HOS,
                       case when ar.REG_TYPE = 3
                              then case when aar.ID is not null and aar.BEGIN_DATE is not null
                                          then to_char(aar.BEGIN_DATE,'dd.mm.yyyy')
                                        else  to_char(ar.BEGIN_DATE,'dd.mm.yyyy')
                                   end
                            when (ar.REG_TYPE in (1,2) OR ar.REG_TYPE IS NULL) and aar.ID is null
                              then to_char(ar.BEGIN_DATE,'dd.mm.yyyy')
                            ELSE to_char(ar.BEGIN_DATE,'dd.mm.yyyy')
                       END DATA_ATTAC,
                       case when ar.REG_TYPE = 3
                              then aar.REG_DOC_NUMB
                            when (ar.REG_TYPE in (1,2) OR ar.REG_TYPE IS NULL) and aar.ID is null
                              then ar.REG_DOC_NUMB
                       END NOTES,
                       case WHEN ar.REG_TYPE = 1
                               then 1
                             WHEN ar.REG_TYPE = 3
                               then 2
                             WHEN ar.REG_TYPE = 2
                               THEN 3
                             WHEN ar.REG_TYPE IS NULL
                                  AND ar.ID IS NOT NULL
                               THEN 4
                        END FID_PERSON,
                        ar.LPU_SITE PRIB_ID,
                        COALESCE(o_s.OID, o_d.oid) SP_MO,
                        CASE WHEN o_s.OID IS NOT NULL
                               THEN d_s.DIV_CODE
                             ELSE d.DIV_CODE
                        END PODR,
                       rp.RP_CODE,
                       CASE rp.RP_CODE
                         WHEN '1' THEN 1
                         WHEN '2' THEN 1
                         WHEN '3' THEN 2
                         WHEN '4' THEN 3
                         ELSE null
                       END RP_TYPE
                  FROM D_AGENT_REGISTRATION ar
                       JOIN AGENTS aa ON aa.ID = ar.PID
                       JOIN D_REGISTER_PURPOSES rp ON ar.REGISTER_PURPOSE = rp.ID
                       JOIN D_LPUDICT ld on ld.ID = ar.LPU_REG
                       JOIN D_LPU l on l.LPUDICT = ld.ID
                       left join D_AGENT_APPLICATION_REG aar on aar.ID = ar.AGENT_APPLICATION_REG

                       left join D_DIVISIONS d on d.ID = ar.DIVISION
                       LEFT JOIN OIDS o_d ON o_d.id = d.id

                       LEFT JOIN D_SITES s ON ar.LPU_SITE = s.ID
                       LEFT JOIN D_DIVISIONS d_s ON d_s.id = s.DIVISION
                       LEFT JOIN OIDS o_s ON o_s.id = s.DIVISION
                 WHERE ar.BEGIN_DATE <= trunc(to_date(:DATE_REESTR,'dd.mm.yyyy'))
                       and (ar.END_DATE is null or ar.END_DATE >= trunc(to_date(:DATE_REESTR,'dd.mm.yyyy')))
                       AND ar.LPU_REG in (SELECT ID FROM MO_LIST)
                ) T
         WHERE T.RP_TYPE IS NOT nULL
                AND ((T.RP_TYPE = 1
                      AND t.LPU_REG in (SELECT ID FROM MO_LIST_AMB))
                       OR T.RP_TYPE <> 1)
        ) t
 WHERE T.MAX_AR_BY_TYPE = T.ID