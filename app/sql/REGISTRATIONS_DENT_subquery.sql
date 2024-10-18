SELECT t.ID,
       t.SURNAME,
       t.FIRSTNAME,
       t.LASTNAME,
       t.BIRTHDATE,
       t.ENP,
       T.CONTACTS,
       T.REGISTRATION_ADDR,
       T.RESIDENTIAL_ADDR,
       T.REF_ID_HOS,
       T.DATA_ATTAC,
       T.NOTES,
       T.FID_PERSON,
       t.SP_MO,
       T.PRIB_ID,
       T.PODR,
       T.REF_ID_DEN,
       T.DATA_DENT,
       T.NOTES_DENT,
       t.FID_DENT_S,
       CASE WHEN t.SITE_OID IS NOT NULL
              THEN  t.SITE_OID
            ELSE D_PKG_FED_NSI_LINKS.GET_CURRENT_VAL(pnRAISE       => 0,
                                                     pnLPU         => t.DIV_LPU,
                                                     psFN_UNITCODE => 'FN_FRMO_DIV',
                                                     psUNITCODE    => 'DIVISIONS',
                                                     psUNIT_ID     =>  t.DIV_ID,
                                                     psFIELD       => 'DEPART_OID')
       END SP_MO_DENT

  FROM (SELECT A.*,
               ld.LPU_CODE REF_ID_DEN,
               case when ar.REG_TYPE = 3
                      then case when aar.ID is not null
                                  then aar.BEGIN_DATE
                                else ar.BEGIN_DATE
                           end
                    when ar.REG_TYPE = 1 and aar.ID is null
                      then ar.BEGIN_DATE
               END DATA_DENT,
               case when ar.REG_TYPE = 3
                      then aar.REG_DOC_NUMB
                    when ar.REG_TYPE = 1 and aar.ID is null
                      then ar.REG_DOC_NUMB
               END NOTES_DENT,
               case WHEN ar.REG_TYPE = 1
                       then 1
                     WHEN ar.REG_TYPE = 3
                       then 2
                     WHEN ar.REG_TYPE = 2
                       THEN 3
                     WHEN ar.REG_TYPE IS NULL
                          AND ar.ID IS NOT NULL
                       THEN 4
                END FID_DENT_S,

                d.ID DIV_ID,
                d.LPU DIV_LPU,
                D_PKG_FED_NSI_LINKS.GET_CURRENT_VAL(pnRAISE       => 0,
                                                    pnLPU         => s.LPU,
                                                    psFN_UNITCODE => 'FN_FRMO_DIV',
                                                    psUNITCODE    => 'DIVISIONS',
                                                    psUNIT_ID     =>  s.DIVISION,
                                                    psFIELD       => 'DEPART_OID') SITE_OID
         FROM REGISTATIONS_HOS a
               LEFT JOIN AGENT_REGISTRATION ar ON ar.PID = A.ID
                                                      AND ar.RP_TYPE = 3
               LEFT JOIN D_LPUDICT ld on ld.ID = ar.LPU_REG
               LEFT JOIN D_LPU l on l.LPUDICT = ld.ID
               left join D_DIVISIONS d on d.ID = ar.DIVISION
               left join D_AGENT_APPLICATION_REG aar on aar.ID = ar.AGENT_APPLICATION_REG
               LEFT JOIN D_SITES s ON ar.LPU_SITE = s.ID
              -- LEFT JOIN D_DIVISIONS d_s ON d_s.id = s.DIVISION
      ) t