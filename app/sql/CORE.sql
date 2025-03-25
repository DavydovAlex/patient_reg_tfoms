SELECT t.*,
       (SELECT OID FROM SMP_OIDS_TO_REPLACE otr WHERE otr.LPU_CODE = t.REF_ID_HOS) SMP_OID
  FROM (SELECT a.ID REF_ID_PER,
               a.SURNAME SNAME,
               a.FIRSTNAME NAME,
               a.LASTNAME MIDDLE_NAM,
               to_char(a.BIRTHDATE,'dd.mm.yyyy') DATE_BIRTH,
               a.ENP,
               a.CONTACTS,
               a.REGISTRATION_ADDR,
               a.RESIDENTIAL_ADDR,
               to_char(add_months(trunc (SYSDATE, 'MM'),1),'dd.mm.yyyy') DATA_REEST,
               CASE WHEN ar_hos.REF_ID_HOS IS NULL
                      THEN CASE WHEN ar_gin.IS_AMB = 1
                                  THEN ar_gin.REF_ID_HOS
                                WHEN ar_dent.IS_AMB = 1 AND (ar_gin.IS_AMB = 0 OR ar_gin.IS_AMB IS null)
                                  THEN ar_dent.REF_ID_HOS
                                ELSE NULL
                           END
                    ELSE ar_hos.REF_ID_HOS
               END REF_ID_HOS,
               CASE WHEN ar_hos.REF_ID_HOS IS NULL
                      THEN CASE WHEN ar_gin.IS_AMB = 1
                                  THEN ar_gin.DATA_ATTAC
                                WHEN ar_dent.IS_AMB = 1 AND (ar_gin.IS_AMB = 0 OR ar_gin.IS_AMB IS null)
                                  THEN ar_dent.DATA_ATTAC
                                ELSE NULL
                           END
                    ELSE ar_hos.DATA_ATTAC
               END DATA_ATTAC,
               ar_hos.NOTES,
               CASE WHEN ar_hos.REF_ID_HOS IS NULL AND (ar_gin.IS_AMB = 1 OR ar_dent.IS_AMB = 1)
                      THEN 1
                    ELSE ar_hos.FID_PERSON
               END FID_PERSON,
               ar_hos.PRIB_ID,
               CASE WHEN ar_hos.SP_MO IS NOT NULL
                      THEN ar_hos.SP_MO
                    WHEN ar_hos.SP_MO IS NULL AND ar_hos.REF_ID_HOS IS NOT NULL
                      THEN (SELECT OID FROM OIDS_TO_REPLACE otr WHERE otr.LPU_CODE = ar_hos.REF_ID_HOS)
                    WHEN ar_hos.SP_MO IS NULL AND ar_hos.REF_ID_HOS IS NULL AND ar_gin.IS_AMB = 1
                      THEN (SELECT OID FROM OIDS_TO_REPLACE otr WHERE otr.LPU_CODE = ar_gin.REF_ID_HOS)
                    WHEN ar_hos.SP_MO IS NULL AND ar_hos.REF_ID_HOS IS NULL AND (ar_gin.IS_AMB = 0 OR ar_gin.IS_AMB IS null) AND ar_dent.IS_AMB = 1
                      THEN (SELECT OID FROM OIDS_TO_REPLACE otr WHERE otr.LPU_CODE = ar_dent.REF_ID_HOS)
                    ELSE null
               END SP_MO,
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
        WHERE COALESCE(ar_hos.REF_ID_HOS,ar_dent.REF_ID_HOS,ar_gin.REF_ID_HOS) IS NOT NULL
        ) t
 WHERE t.SP_MO IS NOT NULL