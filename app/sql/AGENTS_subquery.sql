SELECT A.ID,
       A.BIRTHDATE,
       coalesce(an.SURNAME,a.SURNAME) SURNAME,
       coalesce(an.FIRSTNAME, a.FIRSTNAME) FIRSTNAME,
       coalesce(an.LASTNAME, a.LASTNAME) LASTNAME,
       {enp_prefix} || A.ENP ENP
  FROM D_AGENTS a
       left join D_AGENT_NAMES an
                on an.PID = a.ID
               and an.BEGIN_DATE <= trunc(to_date(:DATE_REESTR,'dd.mm.yyyy'))
               and (an.END_DATE is null or an.END_DATE >= trunc(to_date(:DATE_REESTR,'dd.mm.yyyy')))
 WHERE a.ID in({agents})
