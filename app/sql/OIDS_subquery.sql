SELECT /*+ MATERIALIZE */
       T.id,
       T.OID
FROM (SELECT d.ID,
       D_PKG_FED_NSI_LINKS.GET_CURRENT_VAL(pnRAISE       => 0,
                                                    pnLPU         => d.LPU,
                                                    psFN_UNITCODE => 'FN_FRMO_DIV',
                                                    psUNITCODE    => 'DIVISIONS',
                                                    psUNIT_ID     =>  d.ID,
                                                    psFIELD       => 'DEPART_OID') OID
  FROM D_DIVISIONS d) t
  WHERE T.oid IS NOT null