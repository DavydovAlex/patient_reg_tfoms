SELECT T.*,
       (SELECT coalesce(D_PKG_AGENT_ADDRS.GET_FULL_ADDRESS_BY_ID(aa.ID, 1), aa.MANUAL_INPUT)
          FROM  D_AGENT_ADDRS aa
         WHERE aa.ID = T.REGISTRATION_ADDR) REGISTRATION_ADDR_TEXT,
       (SELECT aA.CITY
          FROM  D_AGENT_ADDRS aa
         WHERE aa.ID = T.REGISTRATION_ADDR) REGISTRATION_ADDR_CITY,
       (SELECT coalesce(D_PKG_AGENT_ADDRS.GET_FULL_ADDRESS_BY_ID(aa.ID, 1), aa.MANUAL_INPUT)
          FROM  D_AGENT_ADDRS aa
         WHERE aa.ID = T.RESIDENTIAL_ADDR) RESIDENTIAL_ADDR_TEXT,
       (SELECT aA.CITY
          FROM  D_AGENT_ADDRS aa
         WHERE aa.ID = T.RESIDENTIAL_ADDR) RESIDENTIAL_ADDR_CITY
FROM (SELECT A.ID,
             A.BIRTHDATE,
             coalesce(an.SURNAME,a.SURNAME) SURNAME,
             coalesce(an.FIRSTNAME, a.FIRSTNAME) FIRSTNAME,
             coalesce(an.LASTNAME, a.LASTNAME) LASTNAME,
             {enp_prefix} || A.ENP ENP,
             (select listagg(ac.CONTACT, ', ') within group (order by ac.IS_MAIN desc)
                 from D_AGENT_CONTACTS ac
                      join D_CONTACT_TYPES ct on ct.ID = ac.CONTACT_TYPE
                where ac.PID = a.ID
                  and (ac.TMP_END_DATE is null or ac.TMP_END_DATE >= trunc(to_date(:DATE_REESTR,'dd.mm.yyyy')))
                  and ct.CT_CODE in (1, 2, 6)
                  AND ROWNUM<=1) CONTACTS,
              (select aa.ID
                 from D_AGENT_ADDRS aa
                where aa.PID = a.ID
                  and aa.IS_REG = 1
                  AND aa.BEGIN_DATE<=trunc(to_date(:DATE_REESTR,'dd.mm.yyyy'))
                  and (aa.END_DATE is null or aa.END_DATE >= trunc(to_date(:DATE_REESTR,'dd.mm.yyyy')))
                  AND ROWNUM<=1) REGISTRATION_ADDR,
              (select aa.ID
                from D_AGENT_ADDRS aa
               where aa.PID = a.ID
                 and aa.IS_REAL = 1
                 AND aa.BEGIN_DATE<=trunc(to_date(:DATE_REESTR,'dd.mm.yyyy'))
                 and (aa.END_DATE is null or aa.END_DATE >= trunc(to_date(:DATE_REESTR,'dd.mm.yyyy')))
                 AND ROWNUM<=1) RESIDENTIAL_ADDR
        FROM D_AGENTS a
             left join D_AGENT_NAMES an
                      on an.PID = a.ID
                     and an.BEGIN_DATE <= trunc(to_date(:DATE_REESTR,'dd.mm.yyyy'))
                     and (an.END_DATE is null or an.END_DATE >= trunc(to_date(:DATE_REESTR,'dd.mm.yyyy')))
       WHERE a.ID in({agents})) t