SELECT /*+ MATERIALIZE */
       T.*
  FROM (SELECT T.*,
               MAX(T.ID)  OVER(PARTITION BY T.PID, T.RP_TYPE) MAX_AR_BY_TYPE
          FROM (SELECT ar.*,
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