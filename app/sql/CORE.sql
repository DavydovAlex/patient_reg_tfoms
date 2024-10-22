SELECT a.ID REF_ID_PER,
       a.SURNAME SNAME,
       a.FIRSTNAME NAME,
       a.LASTNAME MIDDLE_NAM,
       to_char(a.BIRTHDATE,'dd.mm.yyyy') DATE_BIRTH,
       a.ENP,
       a.CONTACTS,
       a.REGISTRATION_ADDR,
       a.RESIDENTIAL_ADDR,
       to_char(trunc(to_date(:DATE_REESTR,'dd.mm.yyyy')),'dd.mm.yyyy') DATA_REEST,
       ar_hos.REF_ID_HOS REF_ID_HOS,
       ar_hos.DATA_ATTAC,
       ar_hos.NOTES,
       ar_hos.FID_PERSON,
       ar_hos.PRIB_ID,
       ar_hos.SP_MO,
       ar_hos.PODR,

       ar_dent.REF_ID_HOS REF_ID_DEN,
       ar_dent.DATA_ATTAC DATA_DENT,
       ar_dent.NOTES NOTES_DENT,
       ar_dent.FID_PERSON FID_DENT_S,
       ar_dent.SP_MO SP_MO_DENT,

       ar_gin.REF_ID_HOS REF_ID_GIN,
       ar_gin.DATA_ATTAC DATA_GINE,
       ar_gin.NOTES NOTES_GIN,
       ar_gin.FID_PERSON FID_GINE_S,
       ar_gin.SP_MO SP_MO_GINE

 FROM AGENTS a
       LEFT JOIN AGENT_REGISTRATION ar_hos ON ar_hos.PID = A.ID
                                              AND ar_hos.RP_TYPE = 1
       LEFT JOIN AGENT_REGISTRATION ar_dent ON ar_dent.PID = A.ID
                                                      AND ar_dent.RP_TYPE = 3
       LEFT JOIN AGENT_REGISTRATION ar_gin ON ar_gin.PID = A.ID
                                                      AND ar_gin.RP_TYPE = 2
WHERE COALESCE(ar_hos.REF_ID_HOS,ar_dent.REF_ID_HOS,ar_gin.REF_ID_HOS) IS NOT null