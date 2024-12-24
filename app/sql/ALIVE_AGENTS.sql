SELECT A.ID
  FROM D_AGENTS a
 WHERE (a.DEATHDATE is null or a.DEATHDATE >= trunc(to_date(:DATE_REESTR,'dd.mm.yyyy')))
       AND a.ACCURACY_DATE_DEATH IS null
       and not exists(select null
                        from D_CF_DEATH_CONTENTS dc
                             join D_CERTIFICATE_FORMS cf
                               on cf.ID = dc.PID
                              and cf.C_STATE = 4
                              and cf.DATE_OUT < trunc(to_date(:DATE_REESTR,'dd.mm.yyyy'))
                       where dc.AGENT = a.ID
                         and rownum = 1)
       AND A.AGN_TYPE = 1 --Физ лица
  ORDER BY A.SURNAME,
           A.FIRSTNAME,
           A.LASTNAME,
           A.BIRTHDATE