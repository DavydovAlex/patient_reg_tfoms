SELECT ld.id
  FROM D_LPU l
       JOIN D_LPUDICT ld on ld.ID = l.LPUDICT
 WHERE l.ID in({ids})